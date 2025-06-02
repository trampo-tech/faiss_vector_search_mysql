import os
import pickle
from rank_bm25 import BM25Okapi

class BM25Manager:
    def __init__(self, bm25_index_path="bm25_index.pkl", corpus_ids_path="corpus_ids.pkl"):
        self.bm25_index_path = bm25_index_path
        self.corpus_ids_path = corpus_ids_path
        self.bm25_okapi_model = None
        self.corpus_ids = None

    def _build_index(self, corpus_data: list, text_fields: list[str] = ["titulo", "descricao"]):
        print("Building BM25 index...")

        corpus_texts = []
        for item in corpus_data:
            texts_to_join = []
            for field in text_fields:
                if field in item and item[field] is not None:
                    texts_to_join.append(str(item[field]))
                else:
                    print(f"Warning: Field '{field}' not found or is None in item with id {item.get('id', 'Unknown')}. Skipping field.")
            if not texts_to_join:
                print(f"Warning: No text could be extracted for item with id {item.get('id', 'Unknown')} using fields {text_fields}. Skipping item.")
                continue
            corpus_texts.append(" ".join(texts_to_join))

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
            return False ## TODO change this later
        return False

    def initialize_index(self, corpus_data: list, columns: List[str]):  # noqa: F821
        """
        Initializes the BM25 index.
        Tries to load from file first, otherwise builds it from corpus_data.
        """
        if not self._load_index():
            if corpus_data:
                self._build_index(corpus_data, columns)
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
    
    def add_or_update_document(
        self,
        item: dict,
        text_fields: list[str] = ["titulo", "descricao"]
    ):
        """
        Incrementally add one document to the BM25 index.
        """
        if not self.bm25_okapi_model or not self.corpus_ids:
            raise Exception("Index not initialized. Call initialize_index() first.")

        
        doc_id = item["id"]
        texts = [
            str(item[f])
            for f in text_fields
            if f in item and item[f] is not None
        ]
        if not texts:
            raise ValueError(f"No valid text found in new item {item.get('id')}")
        new_tokens = " ".join(texts).split()

        # build a fresh model over old corpus + new doc
        if doc_id not in self.corpus_ids:
            updated_corpus = self.bm25_okapi_model.corpus + [new_tokens]
            self.bm25_okapi_model = BM25Okapi(updated_corpus)
            self.corpus_ids.append(doc_id)
        else:
            filtered_tokens = []
            filtered_ids = []
            for tokens, current_id in zip(self.bm25_okapi_model.corpus, self.corpus_ids):
                if current_id != doc_id:
                    filtered_tokens.append(tokens)
                    filtered_ids.append(current_id)

            self.bm25_okapi_model = BM25Okapi(filtered_tokens + [new_tokens])
            self.corpus_ids = filtered_ids
            self.corpus_ids.append(doc_id)

        self._save_index()
        print(f"Added doc {item['id']} and saved updated BM25 index.")