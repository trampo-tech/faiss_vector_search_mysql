import fastapi
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
import os

from src.bm25.bm25_manager import BM25Manager
from src.faiss.faissManager import Faiss_Manager

# from app.utils.simulate_sql import emulate_rental_listings_db
from src.db.database_connector import DatabaseConnector
from config import Config

app = FastAPI()

logger =  Config.init_logging()

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
    user="root", password="ROOT", database="alugo", host="localhost"
)

sql_db.connect()

faiss_managers: Dict[str, Faiss_Manager] = {}
bm25_managers: Dict[str, BM25Manager] = {}


def init_index_for_table(table_name: str, hybrid: bool, sql_db: DatabaseConnector):
    
    data = sql_db.get_all_from_table(table_name)
    if not data:
        raise RuntimeError(f"No data for table '{table_name}'")

    # 2) FAISS
    if hybrid:
        fm = Faiss_Manager(dimensionality=384)
        faiss_path = os.path.join(Config.indexes_dir, f"{table_name}.index")

        if os.path.exists(faiss_path):
            fm.load_from_file(faiss_path)
            logger.info(f"Loaded index from {faiss_path}.")
        else:
            fm.add_from_list(data)
            fm.save_to_file(faiss_path)

        faiss_managers[table_name] = fm
        # 3) BM25

    bm25 = BM25Manager(
        bm25_index_path=os.path.join(Config.indexes_dir, f"{table_name}_bm25.pkl"),
        corpus_ids_path=os.path.join(Config.indexes_dir, f"{table_name}_ids.pkl"),
    )

    bm25_index_path=os.path.join(Config.indexes_dir, f"{table_name}_bm25.pkl")
    corpus_ids_path=os.path.join(Config.indexes_dir, f"{table_name}_ids.pkl")

    if  os.path.exists(bm25_index_path) and os.path.exists(corpus_ids_path):
        bm25._load_index()
    else: 
        bm25.initialize_index(data)
        bm25_managers[table_name] = bm25

# init the indexes 
init_index_for_table("itens", hybrid=True, sql_db=sql_db)
init_index_for_table("usuarios", hybrid=False, sql_db=sql_db)

def item_to_response(item):
    # Remove 'embedding' and datetime fields
    item = item.copy()
    item.pop("embedding", None)
    item.pop("created_at", None)
    item.pop("updated_at", None)
    item.pop("last_embedding_generated_at", None)
    return RentalListingResponse(**item)


@app.get("/search/{table_name}", response_model=dict)
async def search_items(table_name: str, query: str, top: int = 10):
    """
    Do a hybrid search (BM25 + FAISS) on the named table’s indexes.
    If a FAISS index is not available for the table, it will use BM25 search only.
    """
    
    logger.info(f"Search called on table='{table_name}' query='{query}' top={top}")

   
    if table_name not in bm25_managers:
        raise fastapi.HTTPException(status_code=404, detail=f"No BM25 index found for table '{table_name}'. Cannot perform search.")
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
        logger.info(f"FAISS index not found for table '{table_name}'. Proceeding with BM25 results only.")

    # Combine results: lexical results first, then add unique semantic results.
    # dict.fromkeys preserves order and ensures uniqueness.
    combined = list(dict.fromkeys(lexical_ids + semantic_ids))
    logger.info(f"Combined result count after deduplication: {len(combined)}")
    
    # Assuming 'id_to_item' is accessible and correctly populated for the items of the current 'table_name'.
    # This part remains unchanged from your original snippet.
    results = [item_to_response(id_to_item[r]) for r in combined if r in id_to_item]
    return {"results": results}