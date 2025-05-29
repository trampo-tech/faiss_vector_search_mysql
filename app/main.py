import fastapi
from fastapi import FastAPI 
from pydantic import BaseModel
from typing import List

from app.utils.bm25_manager import BM25Manager
from app.utils.faissManager import Faiss_Manager
from app.utils.simulate_sql import emulate_rental_listings_db
from app.utils.database_connector import DatabaseConnector

app = FastAPI()

# Define Pydantic model for output 
class RentalListingResponse(BaseModel):
    id: int
    nome: str  
    descricao: str
    categoria: str
    preco_diaria: float  
    disponivel: bool 
    


my_mysql_emulation = emulate_rental_listings_db(num_itens=1000)

faiss_manager = Faiss_Manager(dimensionality=384)
faiss_manager.add_from_list(my_mysql_emulation)

bm25_search_manager = BM25Manager()
bm25_search_manager.initialize_index(corpus_data=my_mysql_emulation)

def hybrid_search(
    query_text: str,
    bm25_manager_instance: BM25Manager, 
    faiss_manager_instance: Faiss_Manager,   
    data_source: list, 
    top_n: int = 10
):
    """Combines lexical and semantic search results."""
    lexical_ids = bm25_manager_instance.search(query_text, top_n=top_n)
    print(f"Lexical search found IDs: {lexical_ids}")

    semantic_distances, semantic_ids_matrix = faiss_manager_instance.search_text(
        query_text, top_k=top_n
    )
    semantic_ids = semantic_ids_matrix[0].tolist()
    semantic_ids = [id_ for id_ in semantic_ids if id_ != -1]
    print(
        f"Semantic search found IDs: {semantic_ids} with distances: {semantic_distances[0]}"
    )

    combined_ids = list(
        dict.fromkeys(lexical_ids + semantic_ids)
    )

    final_results = []
    for res_id in combined_ids:
        if 1 <= res_id <= len(data_source):
            item_data = data_source[res_id - 1]
            if item_data["id"] == res_id:
                final_results.append(item_data)
            else:
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

def item_to_response(item):
    # Remove 'embedding' 
    item = item.copy()
    item.pop("embedding", None)
    return RentalListingResponse(**item)

@app.get("/search", response_model=dict)
async def search_items(query: str):
    """
    Searches for items based on a query string using hybrid search.
    """
    if not query:
        raise fastapi.HTTPException(status_code=400, detail="Query parameter 'query' cannot be empty.")

    results = hybrid_search(
        query_text=query,
        bm25_manager_instance=bm25_search_manager,
        faiss_manager_instance=faiss_manager,
        data_source=my_mysql_emulation
    )
    
    api_results = [item_to_response(item) for item in results]
    if not api_results:
        return {"message": "No results found.", "results": []}
    return {"results": api_results}

