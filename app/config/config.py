import logging
from dotenv import load_dotenv
import os


class Config:
    embed_model = "sentence-transformers/all-MiniLM-L6-v2"

    class MySQL:
        user = "root"
        password = "ROOT"
        database = "alugo"
        host = "localhost"

        
    @classmethod
    def init_logging(logging_level=logging.ERROR):
        logging.basicConfig(
            level=logging_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler()
            ]
        )
    