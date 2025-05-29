import logging

class Config:
    embed_model = "sentence-transformers/all-MiniLM-L6-v2"

    @classmethod
    def init_logging(logging_level=logging.ERROR):
        logging.basicConfig(
            level=logging_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler()
            ]
        )
    class MySQL:
        user = ""
        password = ""
        database = ""
        host = ""