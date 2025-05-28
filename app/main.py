import fastapi
from fastapi import FastAPI 
from pydantic import BaseModel 

from bm25_manager import BM25Manager
from faissManager import Faiss_Manager
from simulate_sql import emulate_rental_listings_db

# Initialize FastAPI app
app = FastAPI()

# This data and models will be loaded once when the application starts.
my_mysql_emulation = emulate_rental_listings_db(num_itens=1000)

faiss_manager = Faiss_Manager(dimensionality=384)
faiss_manager.add_from_list(my_mysql_emulation)

bm25_search_manager = BM25Manager()
bm25_search_manager.initialize_index(corpus_data=my_mysql_emulation)


# It's good practice to pass dependencies to functions if they are not part of a class
# or managed by FastAPI's dependency injection.
def hybrid_search(
    query_text: str,
    bm25_manager_instance: BM25Manager, # Renamed to avoid confusion with global
    faiss_manager_instance: Faiss_Manager,   # Renamed to avoid confusion with global
    data_source: list, # Pass the data source
    top_n: int = 10
):
    """Combines lexical and semantic search results."""
    # 1. Lexical Search
    # Use the passed instances
    lexical_ids = bm25_manager_instance.search(query_text, top_n=top_n)
    print(f"Lexical search found IDs: {lexical_ids}")

    # faiss_manager_instance.search_text returns (distances, ids)
    # Use the passed instances
    semantic_distances, semantic_ids_matrix = faiss_manager_instance.search_text(
        query_text, top_k=top_n
    )
    semantic_ids = semantic_ids_matrix[0].tolist()
    # Filter out -1 if Faiss returns it for not enough neighbors
    semantic_ids = [id_ for id_ in semantic_ids if id_ != -1]
    print(
        f"Semantic search found IDs: {semantic_ids} with distances: {semantic_distances[0]}"
    )

    combined_ids = list(
        dict.fromkeys(lexical_ids + semantic_ids)
    )  # Union and preserve order somewhat

    final_results = []
    for res_id in combined_ids:
        # Use the passed data_source
        # Assuming IDs in data_source are 1-based and map directly to index-1
        # If IDs are not sequential or 0-based, you'll need a lookup
        if 1 <= res_id <= len(data_source):
            item_data = data_source[res_id - 1] # ID from FAISS/BM25 is 1-based, list index is 0-based.
            if item_data["id"] == res_id: # Double check ID match
                final_results.append(item_data)
            else: # Fallback lookup if IDs are not perfectly sequential or list got reordered
                found_item = next(
                    (item for item in data_source if item["id"] == res_id), None
                )
                if found_item:
                    final_results.append(found_item)
                else:
                    print(
                        f"Warning: ID {res_id} not found by direct index or fallback lookup in data_source."
                    )
        else:
            print(f"Warning: ID {res_id} out of bounds for data_source.")

    return final_results


@app.get("/search/")
async def search_items(query: str):
    """
    Searches for items based on a query string using hybrid search.
    """
    if not query:
        raise fastapi.HTTPException(status_code=400, detail="Query parameter 'query' cannot be empty.")

    # Use the globally initialized managers and data
    results = hybrid_search(
        query_text=query,
        bm25_manager_instance=bm25_search_manager,
        faiss_manager_instance=faiss_manager,
        data_source=my_mysql_emulation
    )
    
    if not results:
        return {"message": "No results found.", "results": []}
    return {"results": results}

