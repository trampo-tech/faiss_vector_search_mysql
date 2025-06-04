import fastapi
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict
import os

from app.faiss.faissManager import Faiss_Manager
from app.db.database_connector import DatabaseConnector
from app.config import Config, TableConfig
from app.dependencies import get_database

app = FastAPI()

logger = Config.init_logging()


# Test initial connection at startup
def test_initial_connection():
    """Test database connection at startup"""
    try:
        db = DatabaseConnector(
            user=Config.MySQL.user,
            password=Config.MySQL.password,
            database=Config.MySQL.database,
            host=Config.MySQL.host,
        )
        if db.test_connection():
            logger.info("Initial database connection test successful")
            return db
        else:
            raise Exception("Initial database connection test failed")
    except Exception as e:
        logger.error(f"Failed to establish initial database connection: {e}")
        raise e


# Test connection at startup
startup_db = test_initial_connection()


def init_index_for_table(
    table_config: TableConfig, sql_db: DatabaseConnector, allow_load: bool = True
):
    """
    Initializes FAISS index for a given database table if hybrid search is enabled.
    MySQL Full-Text Search is managed by the database itself.

    This function retrieves all data from the specified table. If `hybrid` is True,
    it attempts to load or create a FAISS index. The created/loaded FAISS manager
    is stored in the global `faiss_managers` dictionary.

    Args:
        table_name: The name of the table in the SQL database to index.
        hybrid: A boolean flag indicating whether to initialize a FAISS index
                in addition to the BM25 index.
        sql_db: An instance of DatabaseConnector to interact with the SQL database.    Raises:
        RuntimeError: If no data is found in the specified table.
    """
    table_name = table_config.name
    columns = table_config.columns
    data = []  # Initialize data as empty list

    # Data is only needed if we are building a FAISS index from scratch
    if table_config.hybrid and not (
        allow_load
        and os.path.exists(os.path.join(Config.indexes_dir, f"{table_name}.index"))
    ):
        data = sql_db.get_all_from_table(table_config.name)
        if not data:
            logger.warning(
                f"No data found for table '{table_name}' when attempting to build FAISS index. FAISS index may be empty or fail to build."
            )
    # 2) FAISS
    if table_config.hybrid:
        fm = Faiss_Manager(dimensionality=384)
        faiss_path = os.path.join(Config.indexes_dir, f"{table_name}.index")

        if os.path.exists(faiss_path) and allow_load:
            fm.load_from_file(faiss_path)
            logger.info(f"Loaded FAISS index from {faiss_path}.")
        else:
            if data:  # Only add if data was loaded
                fm.add_from_list(data, text_fields=columns)  # type: ignore
                fm.save_to_file(faiss_path)
                logger.info(f"Built and saved FAISS index to {faiss_path}.")
            else:
                logger.warning(
                    f"No data to build FAISS index for table '{table_name}'. Index will be empty."
                )

        faiss_managers[table_name] = fm


# init indexes
faiss_managers: Dict[str, Faiss_Manager] = {}

for table_config in Config.tables_to_index:
    init_index_for_table(table_config, startup_db)

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
async def search_items(table_name: str, query: str, top: int = 50, db: DatabaseConnector = Depends(get_database)):
    """
    Perform hybrid search (MySQL FTS + FAISS) or FTS-only on the specified table.

    Combines lexical search using MySQL Full-Text Search with semantic search
    using FAISS embeddings if a FAISS index is available.

    Args:
        table_name: Name of the database table to search (must have FTS index)
        query: Search query string
        top: Maximum number of results to return (default: 50)

    Returns:
        dict: Dictionary containing 'results' key with list of matching items

    Raises:
        HTTPException: 404 if table configuration not found.
    """

    logger.info(f"Search called on table='{table_name}' query='{query}' top={top}")
    try:
        table_config = Config.get_table_config(table_name)
    except Exception:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No configuration found for table '{table_name}'. Cannot perform search.",
        )
    lexical_ids = db.search_fulltext(table_name, table_config.columns, query, top)
    logger.debug(f"FTS returned {len(lexical_ids)} ids: {lexical_ids}")

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
            f"FAISS index not found or not configured for table '{table_name}'. Proceeding with FTS results only."
        )

    # Combine results: lexical results first, then add unique semantic results.
    # dict.fromkeys preserves order and ensures uniqueness.
    combined = list(dict.fromkeys(lexical_ids + semantic_ids))
    if not combined:
        return {"results": []}

    logger.info(f"Combined result count after deduplication: {len(combined)}")

    # Fetch full item details for combined IDs
    fetched_items_dict = {
        item["id"]: item for item in db.get_items_by_ids(table_name, combined)
    }
    results = [
        item_to_response(fetched_items_dict[r])
        for r in combined
        if r in fetched_items_dict
    ]
    return {"results": results}


@app.post("/indexes/{table_name}")  # should work also as an update
async def add_to_index(table_name: str, item_id: int, db: DatabaseConnector = Depends(get_database)):
    """
    Add or update an item in the specified table's indexes.
    MySQL FTS is updated automatically by DB on data change.
    Updates FAISS index (if hybrid mode is enabled) with the
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
    try:
        table_config = Config.get_table_config(table_name)
    except Exception:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No index found for table '{table_name}'. Consider adding it to the indexes list.",
        )

    # Fetch the item from the database
    item_list = db.get_with_id(item_id, table_name)
    if not item_list:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Item with id {item_id} not found in table {table_name}",
        )
    item = item_list[0] # type: ignore

    if table_config.hybrid:
        # Ensure FAISS manager exists for the table
        faiss = faiss_managers[table_name]
        faiss.add_or_update_item(item, table_config.columns) # type: ignore

    return {"message": "Item added/updated successfully."}


@app.post("/indexes/{table_name}/reindex")
async def reindex_table(table_name: str, db: DatabaseConnector = Depends(get_database)):
    """
    Completely rebuild FAISS index for a specific table if hybrid.
    MySQL FTS re-indexing is typically handled by the DB (e.g., OPTIMIZE TABLE).

    Clears existing indexes and rebuilds them from scratch using current database data.
    This operation may take time for large datasets.

    Args:
        table_name: Name of the database table to reindex

    Returns:
        dict: Success message confirming the reindex operation
    """
    table_config = Config.get_table_config(table_name)

    if table_config.hybrid:
        # Clear old FAISS index instance if it exists
        faiss_managers.pop(table_config.name, None)

    init_index_for_table(table_config, db, allow_load=False)

    return {"message": f"{table_name} reindexed successfully."}


@app.post("/indexes/reindex")
async def reindex_tables():
    """
    Rebuild FAISS indexes for all configured hybrid tables.
    MySQL FTS is handled by the database.

    Performs a complete reindex operation on all tables defined in the configuration.
    This is a time-intensive operation that should be used sparingly.

    Returns:
        dict: Success message confirming all tables have been reindexed
    """
    for table_config in Config.tables_to_index:
        await reindex_table(table_config.name)

    return {"message": "All tables reindexed successfully."}


@app.get("/indexes/omnisearch")
async def omnisearch(query: str, top: int = 25, tables: List[str] = ["itens", "users"], db: DatabaseConnector = Depends(get_database)):
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
        try:
            result[table] = await search_items(
                table, query, top, db
            )  # Pass db parameter
        except HTTPException as e:
            result[table] = {"error": e.detail, "status_code": e.status_code}

    return result
