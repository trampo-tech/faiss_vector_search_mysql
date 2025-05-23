import os
import pickle
from rank_bm25 import BM25Okapi

class BM25Manager:
    def __init__(self, bm25_index_path="bm25_index.pkl", corpus_ids_path="corpus_ids.pkl"):
        self.bm25_index_path = bm25_index_path
        self.corpus_ids_path = corpus_ids_path
        self.bm25_okapi_model = None
        self.corpus_ids = None

    def _build_index(self, corpus_data: list):
        print("Building BM25 index...")
        corpus_texts = [(listing["titulo"] + " " + listing["descricao"]).lower() for listing in corpus_data]
        tokenized_corpus = [doc.split(" ") for doc in corpus_texts]
        self.bm25_okapi_model = BM25Okapi(tokenized_corpus)
        self.corpus_ids = [listing["id"] for listing in corpus_data]
        self._save_index()
        print("BM25 index built and saved.")

    def _save_index(self):
        if self.bm25_okapi_model and self.corpus_ids:
            with open(self.bm25_index_path, "wb") as f_bm25, open(self.corpus_ids_path, "wb") as f_ids:
                pickle.dump(self.bm25_okapi_model, f_bm25)
                pickle.dump(self.corpus_ids, f_ids)
            print(f"BM25 index saved to {self.bm25_index_path} and {self.corpus_ids_path}")

    def _load_index(self):
        if os.path.exists(self.bm25_index_path) and os.path.exists(self.corpus_ids_path):
            print("Loading BM25 index from files...")
            with open(self.bm25_index_path, "rb") as f_bm25, open(self.corpus_ids_path, "rb") as f_ids:
                self.bm25_okapi_model = pickle.load(f_bm25)
                self.corpus_ids = pickle.load(f_ids)
            print("BM25 index loaded.")
            return True
        return False

    def initialize_index(self, corpus_data: list):
        """
        Initializes the BM25 index.
        Tries to load from file first, otherwise builds it from corpus_data.
        """
        if not self._load_index():
            if corpus_data:
                self._build_index(corpus_data)
            else:
                raise ValueError("Corpus data must be provided if index files do not exist.")
        elif not self.bm25_okapi_model or not self.corpus_ids: # If loading failed partially or files were empty
             if corpus_data:
                print("Failed to load a complete index. Rebuilding...")
                self._build_index(corpus_data)
             else:
                raise ValueError("Corpus data must be provided as index files are incomplete or missing.")


    def search(self, query_text: str, top_n: int = 5):
        if not self.bm25_okapi_model or not self.corpus_ids:
            raise Exception("BM25 index not initialized. Call initialize_index() first.")

        query_terms = query_text.lower().split()
        doc_scores = self.bm25_okapi_model.get_scores(query_terms)

        scored_listings = []
        for i, score in enumerate(doc_scores):
            if score > 0: # Consider only documents with a positive score
                # Ensure index i is valid for self.corpus_ids
                if i < len(self.corpus_ids):
                    scored_listings.append((score, self.corpus_ids[i]))
                else:
                    print(f"Warning: Score index {i} out of bounds for corpus_ids (length {len(self.corpus_ids)}).")


        scored_listings.sort(key=lambda x: x[0], reverse=True)
        results_ids = [listing_id for score, listing_id in scored_listings[:top_n]]
        return results_ids