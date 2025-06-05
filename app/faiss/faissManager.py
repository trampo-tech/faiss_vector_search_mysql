import faiss
import numpy
from sentence_transformers import SentenceTransformer
from app.config import Config
import logging
from typing import List, Optional

logger = logging.getLogger()


class Faiss_Manager:
    def __init__(self, dimensionality: int):
        base_index = faiss.IndexFlatL2(dimensionality)
        # Wrap it with IndexIDMap to store custom IDs
        self.index = faiss.IndexIDMap(base_index)
        self.embedding_model = SentenceTransformer(Config.embed_model)

    def save_to_file(self, path: str):
        faiss.write_index(self.index, path)

    def load_from_file(self, path: str):
        self.index = faiss.read_index(path)

    def _add_text(self, text: str, item_id: int):
        embedding = self.embedding_model.encode([text.lower()])  # Lowercase text
        # FAISS expects IDs to be a numpy array of int64
        ids_to_add = numpy.array([item_id], dtype=numpy.int64)
        self.index.add_with_ids(embedding, ids_to_add)  # type: ignore # pylance complains here about something bogus

    def add_from_list(
        self, list_items: list, text_fields: list[str] = ["titulo", "descricao"]
    ):
        # TODO Add verification if Id is already present, if so delete maybe?
        for item in list_items:
            # Concatenate text from specified fields
            texts_to_join = []
            for field in text_fields:
                if field in item and item[field] is not None:
                    texts_to_join.append(str(item[field]).lower())  # Lowercase field text
                else:
                    logger.warning(
                        f"Warning: Field '{field}' not found or is None in item with id {item.get('id', 'Unknown')}. Skipping field."
                    )

            if not texts_to_join:
                logger.warning(
                    f"No text could be extracted for item with id {item.get('id', 'Unknown')} using fields {text_fields}. Skipping item."
                )
                continue

            text_to_embed = " ".join(texts_to_join)
            item_id = item["id"]
            self._add_text(text_to_embed, item_id)

    def add_or_update_item(
        self, item: dict, text_fields: list[str] = ["titulo", "descricao"]
    ):
        if "id" not in item:
            raise ValueError(f"Item does not have an 'id' field. Received: {item}")

        item_id = item["id"]

        # Concatenate text from specified fields
        texts_to_join = []
        for field in text_fields:
            if field in item and item[field] is not None:
                texts_to_join.append(str(item[field]).lower())  # Lowercase field text
            else:
                logger.warning(
                    f"Field '{field}' not found or is None in item with id {item_id}. Skipping field."
                )

        if not texts_to_join:
            raise ValueError(
                f"Warning: No text could be extracted for item with id {item_id} using fields {text_fields}. Skipping item."
            )

        text_to_embed = " ".join(texts_to_join)

        # Remove the old entry if it exists
        # FAISS expects IDs to be a numpy array of int64 for IDSelectorArray
        ids_to_remove_np = numpy.array([item_id], dtype=numpy.int64)
        # Create an IDSelector for the ID to be removed
        # The first argument to IDSelectorArray is the number of IDs,
        # the second is a pointer to the C-array of IDs.
        selector = faiss.IDSelectorArray(
            ids_to_remove_np.shape[0], faiss.swig_ptr(ids_to_remove_np)
        )
        self.index.remove_ids(selector)  # type: ignore

        self._add_text(text_to_embed, item_id)

    def search_text(self, text: str, top_k: int = 5):
        embedding = self.embedding_model.encode([text.lower()])  # Lowercase query text
        return self.index.search(x=embedding, k=top_k)  # type: ignore # pylance complains here about something bogus

    def search_text_with_filter(
        self, text: str, filter_ids: Optional[List[int]] = None, top_k: int = 5
    ):
        """
        Search with optional ID filtering using IDSelector.
        """
        if not hasattr(self, "index") or self.index is None:
            raise ValueError("FAISS index is not initialized.")

        if self.index.ntotal == 0:
            logger.warning("FAISS index is empty.")
            # Return empty arrays in the shape FAISS search normally returns
            return numpy.array([[]], dtype=numpy.float32), numpy.array(
                [[]], dtype=numpy.int64
            )

        logger.info(f"Generating embedding for query text: {text}")
        embedding = self.embedding_model.encode([text.lower()])  # Lowercase query text, Encode returns a 2D array

        if filter_ids is not None and len(filter_ids) > 0:
            logger.info(f"Applying filter with IDs: {filter_ids}")
            ids_array = numpy.array(filter_ids, dtype=numpy.int64)

            selector = faiss.IDSelectorArray(
                ids_array.shape[0], faiss.swig_ptr(ids_array)
            )

            search_params = faiss.SearchParameters()
            search_params.sel = selector

            logger.info("Performing filtered FAISS search.")
            distances, indices = self.index.search(
                x=embedding, k=top_k, params=search_params
            )  # type: ignore
        else:
            logger.info("Performing regular FAISS search without filtering.")
            distances, indices = self.index.search(x=embedding, k=top_k)  # type: ignore

        logger.info(
            f"FAISS search completed. Distances: {distances}, Indices: {indices}"
        )
        return distances, indices
