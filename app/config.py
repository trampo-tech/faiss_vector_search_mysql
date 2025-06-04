from dataclasses import dataclass, field
from typing import List, Optional
import logging


@dataclass(frozen=True)
class FilterConfig:
    column: str
    filter_type: str  # 'exact', 'range', 'in', 'like'
    data_type: str    # 'int', 'string', 'decimal', 'enum', 'date'
    valid_enum_values: Optional[List[str]] = None 

@dataclass(frozen=True)
class TableConfig:
    name: str
    columns: List[str]
    hybrid: bool
    filters: Optional[List[FilterConfig]] = field(default_factory=lambda: [])


class Config:
    embed_model = "sentence-transformers/all-MiniLM-L6-v2"
    indexes_dir = "indexes"

    tables_to_index: List[TableConfig] = [
        TableConfig(name="usuarios", columns=["nome"], hybrid=False, filters=[
            FilterConfig("tipo_usuario", "in", "enum"),
            FilterConfig("data_criacao", "range", "date"),
            FilterConfig("status", "in", "enum", valid_enum_values=["disponivel", "alugado", "manutencao"]),

        ]),
        TableConfig( # Assuming 'itens' table has FULLTEXT index on titulo, descricao, condicoes_uso
            name="itens", columns=["titulo", "descricao", "condicoes_uso"], hybrid=True,
            filters=[
                FilterConfig("categoria_id", "exact", "int"),
                FilterConfig("categoria", "exact", "string"),
                FilterConfig("status", "in", "enum",  valid_enum_values=["ativo", "inativo"]),
                FilterConfig("preco_diario", "range", "decimal"),
                FilterConfig("usuario_id", "exact", "int"),
                FilterConfig("created_at", "range", "date"),
            ]
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
        password = ""
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
