import fastapi

from bm25_manager import BM25Manager
from faissManager import Faiss_Manager
from simulate_sql import emulate_rental_listings_db

my_mysql_emulation = emulate_rental_listings_db(num_itens=1000)

faiss_manager = Faiss_Manager(dimensionality=384)
faiss_manager.add_from_list(my_mysql_emulation)

bm25_search_manager = BM25Manager()
bm25_search_manager.initialize_index(corpus_data=my_mysql_emulation)


def hybrid_search(
    query_text: str,
    bm25: BM25Manager,
    faiss: Faiss_Manager,
    lexical_top_n: int = 5,
    semantic_top_k: int = 5,
):
    """Combines lexical and semantic search results."""
    # 1. Lexical Search

    lexical_ids = bm25_search_manager.search(query_text, top_n=lexical_top_n)
    print(f"Lexical search found IDs: {lexical_ids}")

    # 2. Semantic Search
    # faiss_manager.search_text returns (distances, ids)
    semantic_distances, semantic_ids_matrix = faiss_manager.search_text(
        query_text, top_k=semantic_top_k
    )
    semantic_ids = semantic_ids_matrix[0].tolist()
    # Filter out -1 if Faiss returns it for not enough neighbors
    semantic_ids = [id_ for id_ in semantic_ids if id_ != -1]
    print(
        f"Semantic search found IDs: {semantic_ids} with distances: {semantic_distances[0]}"
    )

    # 3. Combine and Rank (Simple union, then unique)
    # More sophisticated ranking would involve scoring and weighting
    combined_ids = list(
        dict.fromkeys(lexical_ids + semantic_ids)
    )  # Union and preserve order somewhat

    # TODO Change the index handling
    final_results = []
    for res_id in combined_ids:
        # Assuming IDs in my_mysql_emulation are 1-based and map directly to index-1
        # If IDs are not sequential or 0-based, you'll need a lookup
        if 1 <= res_id <= len(my_mysql_emulation):
            # Ensure the item exists and then append.
            # The ID from FAISS/BM25 is 1-based, list index is 0-based.
            item_data = my_mysql_emulation[res_id - 1]
            # Double check if the ID actually matches, in case of non-sequential IDs in the future
            if item_data["id"] == res_id:
                final_results.append(item_data)
            else:
                found_item = next(
                    (item for item in my_mysql_emulation if item["id"] == res_id), None
                )
                if found_item:
                    final_results.append(found_item)
                else:
                    print(
                        f"Warning: ID {res_id} not found directly or by lookup in my_mysql_emulation."
                    )
        else:
            print(f"Warning: ID {res_id} out of bounds for my_mysql_emulation.")

    return final_results


text = "Playstation 5'"
hybrid_results = hybrid_search(text)

print("\nHybrid Search Results:")
for result_listing in hybrid_results:
    print(
        f"ID: {result_listing['id']}, Titulo: {result_listing['titulo']}, Preco Diario: {result_listing['preco_diario']}"
    )
