import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any
import logging

logger = logging.getLogger()

class DatabaseConnector:
    """
    This isnt particularly save but well...
    """
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                logger.info("Connected to the database.")
        except Error as e:
            logger.error(f"Error while connecting to database: {e}")
            self.connection = None

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed.")

    def execute_query(self, query: str, params=None):
        if not self.connection or not self.connection.is_connected():
            logger.warning("Not connected to the database.")
            return None
        cursor = self.connection.cursor(dictionary=True) # type: ignore
        try:
            cursor.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
                logger.info(f"Found {len(result)}")
                return result
            else:
                self.connection.commit()
                return cursor.rowcount
        except Error as e:
            logger.error(f"Error executing query: {e}")
            return None
        finally:
            cursor.close()
    def get_all_from_table(self, table_name: str):
        """
        Retrieves all rows from a specified table.

        Args:
            table_name (str): The name of the table to fetch data from.

        Returns:
            list: A list of dictionaries representing the rows, or None if an error occurs.
        """
        if not table_name:
            logger.warning("Table name cannot be empty.")
            return None
        # Basic protection against SQL injection for table name,
        # ideally, table names come from a controlled source or are validated more strictly.
        if not (table_name.replace('_', '').isalnum()):
            logger.warning(f"Invalid table name: {table_name}")
            return None

        query = f"SELECT * FROM {table_name}" 
        return self.execute_query(query)
    
    def get_with_id(self, id: int, table_name: str):
        """
        Retrieves a single row from a specified table by ID.

        Args:
            id (int): The ID of the row to fetch.
            table_name (str): The name of the table to fetch data from.

        Returns:
            list: A list containing the matching row as a dictionary, or None if an error occurs.
        """
        if not table_name:
            logger.warning("Table name cannot be empty.")
            return None
        # Basic protection against SQL injection for table name,
        # ideally, table names come from a controlled source or are validated more strictly.
        if not (table_name.replace('_', '').isalnum()):
             logger.warning(f"Invalid table name: {table_name}")
             return None

        query = f"SELECT * FROM {table_name} WHERE id = %s" 
        return self.execute_query(query, (id,))

    def get_items_by_ids(self, table_name: str, ids: List[int]) -> List[Dict[str, Any]]:
        """
        Retrieves multiple rows from a specified table by a list of IDs.

        Args:
            table_name (str): The name of the table to fetch data from.
            ids (List[int]): A list of IDs to fetch.

        Returns:
            list: A list of dictionaries representing the rows, or an empty list if no items are found or an error occurs.
        """
        if not ids:
            return []
        if not table_name or not (table_name.replace('_', '').isalnum()):
            logger.warning(f"Invalid table name: {table_name}")
            return []

        placeholders = ','.join(['%s'] * len(ids))
        query = f"SELECT * FROM {table_name} WHERE id IN ({placeholders})"
        return self.execute_query(query, tuple(ids)) or []  # type: ignore

    def search_fulltext(self, table_name: str, search_columns: List[str], query_text: str, top_n: int) -> List[int]:
        """
        Performs a full-text search on the specified table and columns.

        Args:
            table_name (str): The name of the table to search.
            search_columns (List[str]): A list of column names to include in the MATCH() clause.
            query_text (str): The text to search for.
            top_n (int): The maximum number of results to return.

        Returns:
            List[int]: A list of IDs of the matching documents, ordered by relevance.
        """
        if not table_name or not (table_name.replace('_', '').isalnum()):
            logger.warning(f"Invalid table name: {table_name}")
            return []
        if not search_columns:
            logger.warning("Search columns cannot be empty for full-text search.")
            return []
        
        columns_str = ", ".join(search_columns)
        
        # For short queries or single characters, use Boolean mode with wildcard
        if len(query_text.strip()) <= 3:
            # Escape special characters and add wildcard for prefix matching
            escaped_query = query_text.replace('+', '\\+').replace('-', '\\-').replace('(', '\\(').replace(')', '\\)')
            search_query = f"{escaped_query}*"
            sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN BOOLEAN MODE) LIMIT %s"
        else:
            # Use natural language mode for longer queries
            search_query = query_text
            sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN NATURAL LANGUAGE MODE) LIMIT %s"
        
        results = self.execute_query(sql_query, (search_query, top_n))
        return [row['id'] for row in results] if results else []