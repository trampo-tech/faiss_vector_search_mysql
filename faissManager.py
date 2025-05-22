import faiss
import numpy
from sentence_transformers import SentenceTransformer


class Faiss_Manager:
    def __init__(self, dimensionality: int):
        base_index = faiss.IndexFlatL2(dimensionality)
        # Wrap it with IndexIDMap to store custom IDs
        self.index = faiss.IndexIDMap(base_index)
        self.embedding_model = SentenceTransformer(
            "sentence-transformers/average_word_embeddings_glove.840B.300d"
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

    def add_from_list(self, list_items: list):
        # TODO Add verification if Id is already present, if so delete maybe?
        for item in list_items:
            text_to_embed = item["title"] + " " + item["description"]
            item_id = item["id"]
            self._add_text(text_to_embed, item_id)


    def search_text(self, text:str, top_k:int=5):
        embedding = self.embedding_model.encode([text])
        return self.index.search(x=embedding,k=top_k) # type: ignore # pylance complains here about something bogus


