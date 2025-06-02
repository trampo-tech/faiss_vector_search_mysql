import fastapi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import os

from app.bm25.bm25_manager import BM25Manager
from app.faiss.faissManager import Faiss_Manager

# from app.utils.simulate_sql import emulate_rental_listings_db
from app.db.database_connector import DatabaseConnector
from config import Config, TableConfig

app = FastAPI()

logger = Config.init_logging()


sql_db = DatabaseConnector(
    user="root", password="ROOT", database="alugo", host="localhost"
)

sql_db.connect()


def init_index_for_table(
    table_config: TableConfig, sql_db: DatabaseConnector, allow_load: bool = True
):
    """
    Initializes FAISS and/or BM25 indexes for a given database table.

    This function retrieves all data from the specified table. If `hybrid` is True,
    it attempts to load or create a FAISS index. It always attempts to load or
    create a BM25 index. The created/loaded index managers are stored in
    global dictionaries `faiss_managers` and `bm25_managers`.

    Args:
        table_name: The name of the table in the SQL database to index.
        hybrid: A boolean flag indicating whether to initialize a FAISS index
                in addition to the BM25 index.
        sql_db: An instance of DatabaseConnector to interact with the SQL database.

    Raises:
        RuntimeError: If no data is found in the specified table.
    """
    table_name = table_config.name
    columns = table_config.columns
    indexes_dir = Config.indexes_dir

    data = sql_db.get_all_from_table(table_config.name)
    if not data:
        raise RuntimeError(f"No data for table '{table_name}'")

    # 2) FAISS
    if table_config.hybrid:
        fm = Faiss_Manager(dimensionality=384)
        faiss_path = os.path.join(Config.indexes_dir, f"{table_name}.index")

        if os.path.exists(faiss_path) and allow_load:
            fm.load_from_file(faiss_path)
            logger.info(f"Loaded index from {faiss_path}.")
        else:
            fm.add_from_list(data, text_fields=columns)  # type: ignore
            fm.save_to_file(faiss_path)

        faiss_managers[table_name] = fm
        # 3) BM25

    bm25 = BM25Manager(
        bm25_index_path=os.path.join(indexes_dir, f"{table_name}_bm25.pkl"),
        corpus_ids_path=os.path.join(indexes_dir, f"{table_name}_ids.pkl"),
    )

    bm25_index_path = os.path.join(indexes_dir, f"{table_name}_bm25.pkl")
    corpus_ids_path = os.path.join(indexes_dir, f"{table_name}_ids.pkl")

    if (
        os.path.exists(bm25_index_path)
        and os.path.exists(corpus_ids_path)
        and allow_load
    ):
        bm25._load_index()
    else:
        bm25.initialize_index(data, columns=columns)  # type: ignore
        bm25_managers[table_name] = bm25


# init indexes
faiss_managers: Dict[str, Faiss_Manager] = {}
bm25_managers: Dict[str, BM25Manager] = {}

for table_config in Config.tables_to_index:
    init_index_for_table(table_config, sql_db)

###############################################################################################
########### ROUTES


class RentalListingResponse(BaseModel):
    id: int
    titulo: str
    descricao: str
    categoria: str | None = None
    preco_diario: float
    condicoes_uso: str | None = None
    status: str
    usuario_id: int


def item_to_response(item):
    """
    Convert a database item to a RentalListingResponse by removing internal fields.
    
    Removes embedding data and timestamp fields that should not be exposed in the API response.
    
    Args:
        item: Dictionary containing item data from the database
        
    Returns:
        RentalListingResponse: Pydantic model instance with cleaned data
    """
    # Remove 'embedding' and datetime fields
    item = item.copy()
    item.pop("embedding", None)
    item.pop("created_at", None)
    item.pop("updated_at", None)
    item.pop("last_embedding_generated_at", None)
    return RentalListingResponse(**item)


@app.get("/indexes/{table_name}", response_model=dict)
async def search_items(table_name: str, query: str, top: int = 50):
    """
    Perform hybrid search (BM25 + FAISS) on the specified table's indexes.
    
    Combines lexical search using BM25 with semantic search using FAISS embeddings.
    If FAISS index is not available for the table, falls back to BM25-only search.
    
    Args:
        table_name: Name of the database table to search
        query: Search query string
        top: Maximum number of results to return (default: 50)
        
    Returns:
        dict: Dictionary containing 'results' key with list of matching items
        
    Raises:
        HTTPException: 404 if no BM25 index found for the specified table
    """

    logger.info(f"Search called on table='{table_name}' query='{query}' top={top}")

    if table_name not in bm25_managers:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No BM25 index found for table '{table_name}'. Cannot perform search.",
        )
    bm = bm25_managers[table_name]

    # lexical search
    lexical_ids = bm.search(query, top_n=top)
    logger.debug(f"BM25 returned {len(lexical_ids)} ids: {lexical_ids}")

    semantic_ids = []
    # semantic search
    if table_name in faiss_managers:
        fm = faiss_managers[table_name]
        distances, id_matrix = fm.search_text(query, top_k=top)
        # Filter out -1 placeholders which indicate no item found for that slot
        semantic_ids = [i for i in id_matrix[0].tolist() if i != -1]
        logger.debug(f"FAISS returned {len(semantic_ids)} ids: {semantic_ids}")
    else:
        logger.info(
            f"FAISS index not found for table '{table_name}'. Proceeding with BM25 results only."
        )

    # Combine results: lexical results first, then add unique semantic results.
    # dict.fromkeys preserves order and ensures uniqueness.
    combined = list(dict.fromkeys(lexical_ids + semantic_ids))
    logger.info(f"Combined result count after deduplication: {len(combined)}")

    # Assuming 'id_to_item' is accessible and correctly populated for the items of the current 'table_name'.
    # This part remains unchanged from your original snippet.
    results = [item_to_response(id_to_item[r]) for r in combined if r in id_to_item]
    return {"results": results}


@app.post("/indexes/{table_name}")  # should work also as an update
async def add_to_index(table_name: str, item_id: int):
    """
    Add or update an item in the specified table's indexes.
    
    Updates both BM25 and FAISS indexes (if hybrid mode is enabled) with the
    specified item from the database.
    
    Args:
        table_name: Name of the database table
        item_id: ID of the item to add/update in the indexes
        
    Returns:
        dict: Success message confirming the operation
        
    Raises:
        HTTPException: 404 if no index configuration found for the specified table
    """
    logger.info(f"Add called on table='{table_name}' id='{item_id}'")

    for table in Config.tables_to_index:
        if table.name == table_name:
            table_config = table
            break
    else:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No index found for table '{table_name}'. Consider adding it to the indexes list.",
        )

    bm = bm25_managers[table_name]
    
    item = sql_db.get_with_id(item_id, table_name) # TODO Look better into this

    bm.add_or_update_document(item, text_fields=table_config.columns)

    if table_config.hybrid:
        faiss = faiss_managers[table_name]
        faiss.add_or_update_item(item, table_config.columns)

    return {"message": "Item added/updated successfully."}


@app.post("/indexes/{table_name}/reindex")
async def reindex_table(table_name: str):
    """
    Completely rebuild indexes for a specific table.
    
    Clears existing indexes and rebuilds them from scratch using current database data.
    This operation may take time for large datasets.
    
    Args:
        table_name: Name of the database table to reindex
        
    Returns:
        dict: Success message confirming the reindex operation
    """
    table_config = Config.get_table_config(table_name)

    # clear old index instance
    bm25_managers.pop(table_config.name)
    if table_config.hybrid:
        faiss_managers.pop(table_config.name)

    init_index_for_table(table_config, sql_db, allow_load=False)

    return {"message": f"{table_name} reindexed successfully."}


@app.post("/indexes/reindex")
async def reindex_tables():
    """
    Rebuild indexes for all configured tables.
    
    Performs a complete reindex operation on all tables defined in the configuration.
    This is a time-intensive operation that should be used sparingly.
    
    Returns:
        dict: Success message confirming all tables have been reindexed
    """
    for table_config in Config.tables_to_index:
        await reindex_table(table_config.name)

    return {"message": "All tables reindexed successfully."}


@app.get("/indexes/omnisearch")
async def omnisearch(query: str, top: int = 25, tables: List[str] = ["itens", "users"]):
    """
    Perform search across multiple tables simultaneously.
    
    Executes the same search query against multiple specified tables and returns
    consolidated results organized by table name.
    
    Args:
        query: Search query string to execute across all tables
        top: Maximum number of results per table (default: 25)
        tables: List of table names to search (default: ["itens", "users"])
        
    Returns:
        dict: Dictionary with table names as keys and search results as values
    """
    # You sure?
    result = {}
    for table in tables:
        result[table] = search_items(table, query, top)

    return result
