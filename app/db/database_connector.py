import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any

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
                print("Connected to the database.")
        except Error as e:
            print(f"Error while connecting to database: {e}")
            self.connection = None

    def test_connection(self) -> bool:
        """
        Test if the database connection is working by executing a simple query.
        
        Returns:
            bool: True if connection is working, False otherwise
        """
        if not self.connection or not self.connection.is_connected():
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Error as e:
            print(f"Connection test failed: {e}")
            return False

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed.")

    def execute_query(self, query: str, params=None):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database.")
            return None
        cursor = self.connection.cursor(dictionary=True) # type: ignore
        try:
            cursor.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return cursor.rowcount
        except Error as e:
            print(f"Error executing query: {e}")
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
            print("Table name cannot be empty.")
            return None
        # Basic protection against SQL injection for table name,
        # ideally, table names come from a controlled source or are validated more strictly.
        if not (table_name.replace('_', '').isalnum()):
            print(f"Invalid table name: {table_name}")
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
            print("Table name cannot be empty.")
            return None
        # Basic protection against SQL injection for table name,
        # ideally, table names come from a controlled source or are validated more strictly.
        if not (table_name.replace('_', '').isalnum()):
             print(f"Invalid table name: {table_name}")
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
            print(f"Invalid table name: {table_name}")
            return []

        placeholders = ','.join(['%s'] * len(ids))
        query = f"SELECT * FROM {table_name} WHERE id IN ({placeholders})"
        return self.execute_query(query, tuple(ids)) or []

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
            print(f"Invalid table name: {table_name}")
            return []
        if not search_columns:
            print("Search columns cannot be empty for full-text search.")
            return []
        columns_str = ", ".join(search_columns)
        sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN NATURAL LANGUAGE MODE) LIMIT %s"
        results = self.execute_query(sql_query, (query_text, top_n))
        return [row['id'] for row in results] if results else []