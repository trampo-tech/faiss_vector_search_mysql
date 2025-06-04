from app.db.database_connector import DatabaseConnector
from app.config import Config
import logging
from typing import Generator

logger = logging.getLogger()


def get_database() -> Generator[DatabaseConnector, None, None]:
    """
    FastAPI dependency that provides a database connection.
    Tests the connection on creation and ensures cleanup.

    Returns:
        Generator[DatabaseConnector, None, None]: A generator that yields a connected database instance

    Raises:
        Exception: If database connection cannot be established
    """
    db = DatabaseConnector(
        user=Config.MySQL.user,
        password=Config.MySQL.password,
        database=Config.MySQL.database,
        host=Config.MySQL.host,
    )

    db.connect()

    if not db.connection:
        db.disconnect()
        raise Exception("Failed to connect to database or connection test failed.")

    logger.info("Database connection established")

    try:
        yield db
    finally:
        logger.info("Cleaning up database connection")
        db.disconnect()
