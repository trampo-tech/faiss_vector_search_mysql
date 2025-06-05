import fastapi
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, create_model
from typing import List, Dict, Any, Type, Optional
import os

from app.faiss.faissManager import Faiss_Manager
from app.db.database_connector import DatabaseConnector
from app.config import Config, TableConfig
from app.dependencies import get_database
from app.filters.filter_handler import FilterHandler

app = FastAPI()

logger = Config.init_logging()


def test_initial_connection():
    """Test database connection at startup"""
    try:
        db = DatabaseConnector(
            user=Config.MySQL.user,
            password=Config.MySQL.password,
            database=Config.MySQL.database,
            host=Config.MySQL.host,
        )
        db.connect()
        if db.connection:
            logger.info("Initial database connection test successful")
            return db
        else:
            raise Exception("Initial database connection test failed")
    except Exception as e:
        logger.error(f"Failed to establish initial database connection: {e}")
        raise e


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


# Remove the hardcoded RentalListingResponse and replace with dynamic creation
def create_response_model(
    table_name: str, sample_item: Dict[str, Any]
) -> Type[BaseModel]:
    """
    Dynamically create a Pydantic model based on the table structure.

    Args:
        table_name: Name of the table
        sample_item: Sample item from the table to infer field types

    Returns:
        Dynamically created Pydantic model class
    """
    # Define fields based on the sample item, excluding internal fields
    fields = {}
    excluded_fields = {
        "embedding",
        "created_at",
        "updated_at",
        "last_embedding_generated_at",
    }

    for key, value in sample_item.items():
        if key not in excluded_fields:
            # Infer type from value
            if isinstance(value, int):
                fields[key] = (int, ...)
            elif isinstance(value, float):
                fields[key] = (float, ...)
            elif isinstance(value, str):
                fields[key] = (str, ...)
            else:
                fields[key] = (Any, None)  # Optional field with Any type

    # Create dynamic model
    model_name = f"{table_name.capitalize()}Response"
    return create_model(model_name, **fields)


def item_to_response(item: Dict[str, Any], table_name: str):
    """
    Convert a database item to a dynamic response model.

    Args:
        item: Dictionary containing item data from the database
        table_name: Name of the table to determine response structure

    Returns:
        Dynamic Pydantic model instance with cleaned data
    """
    # Remove internal fields
    clean_item = item.copy()
    excluded_fields = {
        "embedding",
        "created_at",
        "updated_at",
        "last_embedding_generated_at",
    }
    for field in excluded_fields:
        clean_item.pop(field, None)

    # Create dynamic model and return instance
    ResponseModel = create_response_model(table_name, clean_item)
    return ResponseModel(**clean_item)


async def _reindex_table_internal(table_name: str, db: DatabaseConnector):
    """
    Internal function to reindex a single table.
    Separated from the route handler to allow direct calls.
    """
    table_config = Config.get_table_config(table_name)

    if table_config.hybrid:
        # Clear old FAISS index instance if it exists
        faiss_managers.pop(table_config.name, None)

    init_index_for_table(table_config, db, allow_load=False)

@app.post("/indexes/reindex")
async def reindex_tables(db: DatabaseConnector = Depends(get_database)):
    """
    Rebuild FAISS indexes for all configured hybrid tables.
    MySQL FTS is handled by the database.

    Performs a complete reindex operation on all tables defined in the configuration.
    This is a time-intensive operation that should be used sparingly.

    Returns:
        dict: Success message confirming all tables have been reindexed
    """
    for table_config in Config.tables_to_index:
        await _reindex_table_internal(table_config.name, db)

    return {"message": "All tables reindexed successfully."}


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


@app.get("/indexes/omnisearch")
async def omnisearch(
    query: str = "",
    top: int = 25,
    tables: List[str] = ["itens", "usuarios"],
    filters: Optional[str] = None,
    db: DatabaseConnector = Depends(get_database),
):
    """
    Perform search across multiple tables simultaneously with optional filters.

    Executes the same search query against multiple specified tables and returns
    consolidated results organized by table name. Filters apply to all tables that support them.

    Args:
        query: Search query string to execute across all tables (can be empty)
        top: Maximum number of results per table (default: 25)
        tables: List of table names to search (default: ["itens", "usuarios"])
        filters: Filter string in format "column:value,column2:min-max,column3:val1,val2"

    Returns:
        dict: Dictionary with table names as keys and search results as values
    """
    result = {}
    processed_query = query.lower() if query else ""
    for table in tables:
        try:
            result[table] = await search_items(
                table, processed_query, top, filters, db
            )  # Pass all parameters including filters
        except HTTPException as e:
            result[table] = {"error": e.detail, "status_code": e.status_code}

    return result


@app.get("/indexes/{table_name}", response_model=dict)
async def search_items(
    table_name: str,
    query: str = "",
    top: int = 50,
    filters: Optional[str] = None,
    db: DatabaseConnector = Depends(get_database),
):
    """
    Perform hybrid search with optional filters.
    If query is empty but filters are provided, returns filtered results.

    Args:
        table_name: Name of the database table to search
        query: Search query string (can be empty)
        top: Maximum number of results to return
        filters: Filter string in format "column:value,column2:min-max,column3:val1,val2"
    """
    processed_query = query.lower() if query else ""
    logger.info(
        f"Search called on table='{table_name}' query='{processed_query}' top={top}, filters='{filters}'"
    )

    try:
        table_config = Config.get_table_config(table_name)
    except Exception:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No configuration found for table '{table_name}'. Cannot perform search.",
        )

    # Parse filters
    parsed_filters = FilterHandler.parse_filters(filters or "", table_config)
    logger.debug(f"Parsed filters: {parsed_filters}")

    # Handle empty query case
    if not processed_query or not processed_query.strip():
        if parsed_filters:
            # Return filtered results without search
            lexical_ids = db.get_all_with_filters(table_name, parsed_filters, top)
            logger.debug(
                f"Filtered results (no query) returned {len(lexical_ids)} ids: {lexical_ids}"
            )
        else:
            # Return all results if no query and no filters
            lexical_ids = db.get_all_with_filters(table_name, {}, top)
            logger.debug(
                f"All results (no query, no filters) returned {len(lexical_ids)} ids"
            )

        semantic_ids = []
    else:
        # Get lexical search results with filters
        if parsed_filters:
            lexical_ids = db.search_fulltext_with_filters(
                table_name, table_config.columns, processed_query, parsed_filters, top
            )
        else:
            lexical_ids = db.search_fulltext(
                table_name, table_config.columns, processed_query, top
            )

        logger.debug(f"FTS returned {len(lexical_ids)} ids: {lexical_ids}")

        semantic_ids = []
        # Semantic search with filters
        if table_name in faiss_managers:
            fm = faiss_managers[table_name]

            # Get filtered IDs for FAISS if filters are present
            filter_ids = None
            if parsed_filters:
                filter_ids = db.get_filtered_ids(table_name, parsed_filters)
                logger.debug(
                    f"Filter IDs for FAISS: {len(filter_ids) if filter_ids else 0}"
                )

            distances, id_matrix = fm.search_text_with_filter(
                processed_query, filter_ids, top_k=top
            )
            semantic_ids = [i for i in id_matrix[0].tolist() if i != -1]
            logger.debug(f"FAISS returned {len(semantic_ids)} ids: {semantic_ids}")
        else:
            logger.info(
                f"FAISS index not found for table '{table_name}'. Using FTS only."
            )

    # Combine and return results
    combined = list(dict.fromkeys(lexical_ids + semantic_ids))
    if not combined:
        return {"results": []}

    logger.info(f"Combined result count after deduplication: {len(combined)}")

    fetched_items_dict = {
        item["id"]: item for item in db.get_items_by_ids(table_name, combined)
    }
    results = [
        item_to_response(fetched_items_dict[r], table_name)
        for r in combined
        if r in fetched_items_dict
    ]
    return {"results": results}


@app.post("/indexes/{table_name}")  # should work also as an update
async def add_to_index(
    table_name: str, item_id: int, db: DatabaseConnector = Depends(get_database)
):
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
    item = item_list[0]  # type: ignore

    if table_config.hybrid:
        # Ensure FAISS manager exists for the table
        faiss = faiss_managers[table_name]
        faiss.add_or_update_item(item, table_config.columns)  # type: ignore

    return {"message": "Item added/updated successfully."}



