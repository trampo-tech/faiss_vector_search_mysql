from dataclasses import dataclass
from typing import List
import logging


@dataclass(frozen=True)
class TableConfig:
    name: str
    columns: List[str]
    hybrid: bool


class Config:
    embed_model = "sentence-transformers/all-MiniLM-L6-v2"
    indexes_dir = "indexes"

    tables_to_index: List[TableConfig] = [
        TableConfig(name="usuarios", columns=["nome"], hybrid=False),
        TableConfig(
            name="itens", columns=["titulo", "descricao", "condicoes_uso"], hybrid=True
        ),
    ]

    @classmethod
    def get_table_config(cls, table_name: str) -> TableConfig:
        for table in cls.tables_to_index:
            if table.name == table_name:
                return table
        else:
            raise Exception(f"Did not find config for {table_name}")

    class MySQL:
        user = "root"
        password = "ROOT"
        database = "alugo"
        host = "localhost"

    @classmethod
    def init_logging(cls, logging_level: int = logging.ERROR) -> logging.Logger:
        logging.basicConfig(
            level=logging_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )
        return logging.getLogger()
