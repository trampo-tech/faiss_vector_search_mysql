import faiss
import numpy
from sentence_transformers import SentenceTransformer
from app.config.config import Config

class Faiss_Manager:
    def __init__(self, dimensionality: int):
        base_index = faiss.IndexFlatL2(dimensionality)
        # Wrap it with IndexIDMap to store custom IDs
        self.index = faiss.IndexIDMap(base_index)
        self.embedding_model = SentenceTransformer(
            Config.embed_model
        )

    def save_to_file(self, path: str):
        faiss.write_index(self.index, path)

    def load_from_file(self, path: str):
        self.index = faiss.read_index(path)

    def _add_text(self, text: str, item_id: int):  
        embedding = self.embedding_model.encode([text])
        # FAISS expects IDs to be a numpy array of int64
        ids_to_add = numpy.array([item_id], dtype=numpy.int64)
        self.index.add_with_ids(embedding, ids_to_add)  # type: ignore # pylance complains here about something bogus

    def add_from_list(self, list_items: list, text_fields: list[str] = ["titulo", "descricao"]):
        # TODO Add verification if Id is already present, if so delete maybe?
        for item in list_items:
            # Concatenate text from specified fields
            texts_to_join = []
            for field in text_fields:
                if field in item and item[field] is not None:
                    texts_to_join.append(str(item[field]))
                else:
                    print(f"Warning: Field '{field}' not found or is None in item with id {item.get('id', 'Unknown')}. Skipping field.")
            
            if not texts_to_join:
                print(f"Warning: No text could be extracted for item with id {item.get('id', 'Unknown')} using fields {text_fields}. Skipping item.")
                continue

            text_to_embed = " ".join(texts_to_join)
            item_id = item["id"]
            self._add_text(text_to_embed, item_id)


    def search_text(self, text:str, top_k:int=5):
        embedding = self.embedding_model.encode([text])
        return self.index.search(x=embedding,k=top_k) # type: ignore # pylance complains here about something bogus


