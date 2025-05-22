import logging

class Config:

    @classmethod
    def init_logging(logging_level=logging.ERROR):
        logging.basicConfig(
            level=logging_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler()
            ]
        )