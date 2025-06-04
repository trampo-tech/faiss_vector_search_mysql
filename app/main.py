import fastapi
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

from app.utils.faissManager import Faiss_Manager
# from app.utils.simulate_sql import emulate_rental_listings_db
from app.utils.database_connector import DatabaseConnector
from app.config.config import Config

app = FastAPI()

config = Config


class RentalListingResponse(BaseModel):
    id: int
    titulo: str
    descricao: str
    categoria: str | None = None
    preco_diario: float
    condicoes_uso: str | None = None
    status: str
    usuario_id: int


sql_db = DatabaseConnector(
    user=Config.MySQL.user,
    password=Config.MySQL.password,
    database=Config.MySQL.database,
    host=Config.MySQL.host
)

sql_db.connect()

table_data_from_db = None
if sql_db.connection and sql_db.connection.is_connected():
    print("Attempting to fetch data from the database...")
    table_data_from_db = sql_db.get_all_from_table("itens") 
    if table_data_from_db:
        print(f"Successfully fetched {len(table_data_from_db)} items from the database.")
    else:
        print("Could not fetch data from database, or table is empty.")

table_result = table_data_from_db

if not table_result:
    raise RuntimeError("Failed to load data from database for id_to_item map and Faiss.")


faiss_manager = Faiss_Manager(dimensionality=384)
faiss_manager.add_from_list(table_result)

# Build a mapping from id to item for fast lookup
id_to_item = {item["id"]: item for item in table_result}

def perform_mysql_fulltext_search(
    db_connector: DatabaseConnector,
    query_text: str,
    top_k: int,
    metadata_filters: dict | None = None
) -> List[dict]:  # Returns list of dicts like {'id': X, 'score': Y}
    """
    Performs a MySQL FULLTEXT search with optional metadata filters.
    """
    # Base query parts
    select_clause = "SELECT id, MATCH(titulo, descricao) AGAINST(%s IN NATURAL LANGUAGE MODE) AS score"
    from_clause = "FROM itens"
    # Main FULLTEXT search condition
    where_match_clause = "WHERE MATCH(titulo, descricao) AGAINST(%s IN NATURAL LANGUAGE MODE)"

    params = [query_text, query_text]  # Params for the two AGAINST clauses

    filter_sql_parts = []
    if metadata_filters:
        for key, value in metadata_filters.items():
            # Basic protection: ensure key is alphanumeric with underscores if dynamic
            # For production, validate keys against a known list of filterable columns
            if key.replace('_', '').isalnum():
                filter_sql_parts.append(f"AND {key} = %s")
                params.append(value)

    # Combine WHERE clauses
    full_where_clause = where_match_clause
    if filter_sql_parts:
        full_where_clause += " " + " ".join(filter_sql_parts)

    # Add HAVING clause to ensure only relevant results (score > 0)
    # MATCH in WHERE usually implies this, but explicit HAVING can be clearer
    # or useful if you only put MATCH in SELECT for scoring all rows.
    # For now, relying on MATCH in WHERE to filter non-matches.
    # You could add: "HAVING score > 0" if needed.

    order_by_limit_clause = "ORDER BY score DESC LIMIT %s"
    params.append(top_k)

    final_sql = f"{select_clause} {from_clause} {full_where_clause} {order_by_limit_clause}"

    # For debugging:
    # print(f"Executing FTS SQL: {final_sql}")
    # print(f"With params: {tuple(params)}")

    try:
        results = db_connector.execute_query(final_sql, tuple(params))
        if results:
            return [{"id": int(row["id"]), "score": float(row["score"])} for row in results]
    except Exception as e:
        print(f"Error during MySQL FTS query: {e}")  # Add proper logging
    return []

def hybrid_search(
    query_text: str,
    db_connector: DatabaseConnector,       # ADD THIS PARAMETER (or access sql_db globally)
    faiss_manager_instance: Faiss_Manager,
    id_lookup_map: dict,  # Was data_source, now explicitly for item lookup
    target_top_n: int = 10,
    initial_candidate_pool_multiplier: int = 3,  # Adjust as needed
    metadata_filters: dict | None = None,
):
    """Combines MySQL FULLTEXT lexical and Faiss semantic search results."""

    candidate_fetch_k = target_top_n * initial_candidate_pool_multiplier

    # --- Lexical Search using MySQL FULLTEXT ---
    lexical_results_with_scores = perform_mysql_fulltext_search(
        db_connector=db_connector,  # Use the passed connector
        query_text=query_text,
        top_k=candidate_fetch_k,
        metadata_filters=metadata_filters  # Pass along filters
    )
    lexical_ids = [res['id'] for res in lexical_results_with_scores]
    print(f"Lexical (MySQL FTS) search found IDs: {lexical_ids}")
    # You can also retain lexical_results_with_scores if you want to use FTS scores in ranking

    # --- Semantic Search (No change here) ---
    semantic_distances, semantic_ids_matrix = faiss_manager_instance.search_text(
        query_text, top_k=candidate_fetch_k  # Also fetch more for potential re-ranking/filtering
    )
    semantic_ids = semantic_ids_matrix[0].tolist()
    semantic_ids = [int(id_) for id_ in semantic_ids if id_ != -1]  # Ensure IDs are int
    print(
        f"Semantic search found IDs: {semantic_ids} with distances: {semantic_distances[0]}"
    )

    # --- Combine and De-duplicate Results ---
    # Simple combination for now. Consider scores for more advanced ranking.
    combined_ids_ordered = []
    seen_ids = set()

    # Add lexical results first (maintaining some order)
    for id_val in lexical_ids:
        if id_val not in seen_ids:
            combined_ids_ordered.append(id_val)
            seen_ids.add(id_val)

    # Add semantic results that are not already included
    for id_val in semantic_ids:
        if id_val not in seen_ids:
            combined_ids_ordered.append(id_val)
            seen_ids.add(id_val)

    # --- Fetch full item data for the combined unique IDs ---
    final_results_data = []
    for res_id in combined_ids_ordered[:target_top_n]:  # Limit to target_top_n after combining
        item_data = id_lookup_map.get(res_id)
        if item_data:
            final_results_data.append(item_data)
        else:
            print(f"Warning: ID {res_id} not found in id_lookup_map.")

    return final_results_data  # Return the actual item data

def item_to_response(item):
    # Remove 'embedding' and datetime fields
    item = item.copy()
    item.pop("embedding", None)
    item.pop("created_at", None)
    item.pop("updated_at", None)
    item.pop("last_embedding_generated_at", None)
    return RentalListingResponse(**item)


@app.get("/search", response_model=dict)
async def search_items(query: str, categoria: str | None = None, status: str | None = None):  # Example filters
    """
    Searches for items based on a query string using hybrid search.
    """
    if not query:
        raise fastapi.HTTPException(
            status_code=400, detail="Query parameter 'query' cannot be empty."
        )

    # Construct metadata_filters from query parameters for lexical search
    current_lexical_filters = {}
    if categoria:
        current_lexical_filters["categoria"] = categoria
    if status:
        current_lexical_filters["status"] = status
    # Add more filters as needed, ensuring they match column names in 'itens'

    results_data = hybrid_search(
        query_text=query,
        db_connector=sql_db,  # Pass the global sql_db instance
        faiss_manager_instance=faiss_manager,
        id_lookup_map=id_to_item,
        target_top_n=10,  # Desired final number of results
        initial_candidate_pool_multiplier=3,  # Fetch 3x more candidates initially
        metadata_filters=current_lexical_filters if current_lexical_filters else None,
    )

    api_results = [item_to_response(item) for item in results_data]
    if not api_results:
        return {"message": "No results found matching your criteria.", "results": []}

    # Optionally, include applied filters in the response for clarity
    response_payload = {"results": api_results}
    if current_lexical_filters:
        response_payload["filters_applied"] = current_lexical_filters
    return response_payload
