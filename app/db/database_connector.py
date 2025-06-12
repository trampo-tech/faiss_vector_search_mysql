import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Tuple, Optional # Added Optional
import logging

logger = logging.getLogger()


class DatabaseConnector:
    """
    Handles database connections and queries.
    Note: Table and column names should be carefully validated if they come from user input
    to prevent SQL injection, though this class primarily expects them from configuration.
    """

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection: Optional[mysql.connector.MySQLConnection] = None # Type hint for connection

    def connect(self):
        """Establishes a connection to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            if self.connection and self.connection.is_connected():
                logger.info("Connected to the database.")
            else:
                logger.error("Failed to connect to the database after connect call.")
                self.connection = None # Ensure connection is None if not connected
        except Error as e:
            logger.error(f"Error while connecting to database: {e}")
            self.connection = None

    def disconnect(self):
        """Closes the database connection if it is open."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed.")
        self.connection = None # Ensure connection is set to None after closing

    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]] | int]:
        """
        Executes a given SQL query with optional parameters.

        Args:
            query (str): The SQL query to execute.
            params (Optional[tuple]): A tuple of parameters to bind to the query. Defaults to None.

        Returns:
            Optional[List[Dict[str, Any]] | int]: 
                - For SELECT queries, a list of dictionaries representing the rows.
                - For other queries (INSERT, UPDATE, DELETE), the number of affected rows (rowcount).
                - None if an error occurs or if not connected.
        """
        if not self.connection or not self.connection.is_connected():
            logger.warning("Not connected to the database. Cannot execute query.")
            return None
        
        cursor: Optional[mysql.connector.cursor.MySQLCursorDict] = None # Type hint for cursor
        try:
            cursor = self.connection.cursor(dictionary=True) # type: ignore
            logger.debug(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params or ())
            
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
                logger.info(f"SELECT query returned {len(result)} rows.")
                return result
            else:
                self.connection.commit()
                logger.info(f"Non-SELECT query affected {cursor.rowcount} rows.")
                return cursor.rowcount
        except Error as e:
            logger.error(
                f"Error executing query: {e}\nQuery: {query}\nParams: {params}"
            )
            # Consider rolling back if it's a transactional error, though commit is explicit for non-select
            # if self.connection and query.strip().lower().startswith(("insert", "update", "delete")):
            #     self.connection.rollback()
            return None
        finally:
            if cursor:
                cursor.close()

    def get_all_from_table(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieves all rows from a specified table.

        Args:
            table_name (str): The name of the table to fetch data from.

        Returns:
            Optional[List[Dict[str, Any]]]: A list of dictionaries representing the rows, or None if an error occurs.
        """
        if not table_name:
            logger.warning("Table name cannot be empty for get_all_from_table.")
            return None
        # Basic validation for table name
        if not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for get_all_from_table: {table_name}")
            return None

        query = f"SELECT * FROM `{table_name}`" # Use backticks for safety
        result = self.execute_query(query)
        return result if isinstance(result, list) else None


    def get_with_id(self, item_id: int, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieves a single row from a specified table by ID.

        Args:
            item_id (int): The ID of the row to fetch.
            table_name (str): The name of the table to fetch data from.

        Returns:
            Optional[List[Dict[str, Any]]]: A list containing the matching row as a dictionary (or empty if not found), 
                                           or None if an error occurs.
        """
        if not table_name:
            logger.warning("Table name cannot be empty for get_with_id.")
            return None
        if not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for get_with_id: {table_name}")
            return None

        query = f"SELECT * FROM `{table_name}` WHERE id = %s" # Use backticks
        result = self.execute_query(query, (item_id,))
        return result if isinstance(result, list) else None


    def get_items_by_ids(self, table_name: str, ids: List[int]) -> List[Dict[str, Any]]:
        """
        Retrieves multiple rows from a specified table by a list of IDs.

        Args:
            table_name (str): The name of the table to fetch data from.
            ids (List[int]): A list of IDs to fetch.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the rows, 
                                  or an empty list if no items are found or an error occurs.
        """
        if not ids:
            return []
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for get_items_by_ids: {table_name}")
            return []

        placeholders = ",".join(["%s"] * len(ids))
        query = f"SELECT * FROM `{table_name}` WHERE id IN ({placeholders})" # Use backticks
        result = self.execute_query(query, tuple(ids))
        return result if isinstance(result, list) else []

    def search_fulltext(
        self, table_name: str, search_columns: List[str], query_text: str, top_n: int
    ) -> List[int]:
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
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for search_fulltext: {table_name}")
            return []
        if not search_columns:
            logger.warning("Search columns cannot be empty for full-text search.")
            return []

        columns_str = ", ".join([f"`{col}`" for col in search_columns]) # Use backticks for column names

        # For short queries or single characters, use Boolean mode with wildcard
        processed_query_text = query_text.strip()
        if len(processed_query_text) <= 3:
            logger.info(
                f"Short query detected for FTS. Using Boolean mode with wildcard for query: '{processed_query_text}'"
            )
            # Escape special characters and add wildcard for prefix matching
            escaped_query = (
                processed_query_text.replace("+", "\\+")
                .replace("-", "\\-")
                .replace("(", "\\(")
                .replace(")", "\\)")
                .replace("*", "\\*") # Escape existing wildcards in query
                .replace("?", "\\?")
            )
            search_query = f"{escaped_query}*"
            sql_query = f"SELECT id FROM `{table_name}` WHERE MATCH({columns_str}) AGAINST (%s IN BOOLEAN MODE) LIMIT %s"
        else:
            logger.info(
                f"Long query detected for FTS. Using Natural Language mode for query: '{processed_query_text}'"
            )
            search_query = processed_query_text
            sql_query = f"SELECT id FROM `{table_name}` WHERE MATCH({columns_str}) AGAINST (%s IN NATURAL LANGUAGE MODE) LIMIT %s"

        logger.debug(
            f"Executing full-text search query: {sql_query} with parameters: ('{search_query}', {top_n})"
        )
        results = self.execute_query(sql_query, (search_query, top_n))

        if results and isinstance(results, list):
            logger.info(f"Full-text search returned {len(results)} IDs.")
            return [row["id"] for row in results if "id" in row]
        else:
            logger.warning(
                "Full-text search returned no results or results are not in expected format."
            )
            return []

    def search_fulltext_with_filters(
        self,
        table_name: str,
        search_columns: List[str],
        query_text: str,
        filters: Dict[str, Any],
        top_n: int,
    ) -> List[int]:
        """
        Performs a full-text search with filters on the specified table and columns.

        Args:
            table_name (str): The name of the table to search.
            search_columns (List[str]): A list of column names to include in the MATCH() clause.
            query_text (str): The text to search for.
            filters (Dict[str, Any]): Filters to apply to the search.
            top_n (int): The maximum number of results to return.

        Returns:
            List[int]: A list of IDs of the matching documents.
        """
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for search_fulltext_with_filters: {table_name}")
            return []
        if not search_columns:
            logger.warning("Search columns cannot be empty for full-text search with filters.")
            return []

        columns_str = ", ".join([f"`{col}`" for col in search_columns]) # Use backticks
        filter_sql, filter_params = self._build_filter_conditions(filters)
        
        params: list[Any] = [] # Explicitly type params

        processed_query_text = query_text.strip()
        if len(processed_query_text) <= 3:
            logger.info(
                f"Short query detected for FTS with filters. Using Boolean mode with wildcard for query: '{processed_query_text}'"
            )
            escaped_query = (
                processed_query_text.replace("+", "\\+")
                .replace("-", "\\-")
                .replace("(", "\\(")
                .replace(")", "\\)")
                .replace("*", "\\*")
                .replace("?", "\\?")
            )
            search_query = f"{escaped_query}*"
            sql_query_base = f"SELECT id FROM `{table_name}` WHERE MATCH({columns_str}) AGAINST (%s IN BOOLEAN MODE)"
            params.append(search_query)
        else:
            logger.info(
                f"Long query detected for FTS with filters. Using Natural Language mode for query: '{processed_query_text}'"
            )
            search_query = processed_query_text
            sql_query_base = f"SELECT id FROM `{table_name}` WHERE MATCH({columns_str}) AGAINST (%s IN NATURAL LANGUAGE MODE)"
            params.append(search_query)

        if filter_sql:
            sql_query = f"{sql_query_base} AND {filter_sql}"
            params.extend(filter_params)
        else:
            sql_query = sql_query_base
        
        sql_query += " LIMIT %s"
        params.append(top_n)

        logger.debug(
            f"Executing full-text search query with filters: {sql_query} with parameters: {tuple(params)}"
        )
        results = self.execute_query(sql_query, tuple(params))

        if results and isinstance(results, list):
            logger.info(f"Full-text search with filters returned {len(results)} IDs.")
            return [row["id"] for row in results if "id" in row]
        else:
            logger.warning(
                "Full-text search with filters returned no results or results are not in expected format."
            )
            return []

    def _build_filter_conditions(self, filters: Dict[str, Any]) -> Tuple[str, list]:
        """
        Builds SQL filter conditions and parameters from a dictionary of filters.
        Assumes `filters` comes from `FilterHandler.parse_filters`.
        """
        logger.debug(f"Building filter conditions for: {filters}")
        conditions = []
        params: list[Any] = [] # Explicitly type params
        earth_radius_km = 6371 # Earth radius in kilometers. Use 3959 for miles.

        for filter_key_config_name, filter_detail_wrapper in filters.items(): 
            logger.debug(f"Processing filter for config key '{filter_key_config_name}': {filter_detail_wrapper}")

            if not isinstance(filter_detail_wrapper, dict) or "filter_data" not in filter_detail_wrapper:
                logger.warning(
                    f"Unexpected filter_detail_wrapper format or missing 'filter_data' for filter key '{filter_key_config_name}': {filter_detail_wrapper}. Skipping."
                )
                continue

            filter_data = filter_detail_wrapper["filter_data"] 
            filter_type = filter_detail_wrapper.get("filter_type")
            db_column_name = filter_key_config_name # By default, the filter key is the db column name

            if not isinstance(filter_data, dict):
                logger.warning(
                   f"'filter_data' is not a dictionary for filter key '{filter_key_config_name}': {filter_data}. Skipping."
               )
                continue

            if filter_type == "distance":
                lat_col_name = filter_detail_wrapper.get("latitude_column_name")
                lon_col_name = filter_detail_wrapper.get("longitude_column_name")
                
                center_lat = filter_data.get("center_lat")
                center_lon = filter_data.get("center_lon")
                # The key in filter_data from FilterHandler is "max_distance"
                max_distance_val = filter_data.get("max_distance")


                if lat_col_name and lon_col_name and center_lat is not None and center_lon is not None and max_distance_val is not None:
                    # Haversine formula for distance in SQL
                    # ( R * acos( cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon2) - radians(lon1)) + sin(radians(lat1)) * sin(radians(lat2)) ) )
                    distance_calculation_sql = (
                        f"( {earth_radius_km} * ACOS( COS( RADIANS(%s) ) * COS( RADIANS(`{lat_col_name}`) ) * "
                        f"COS( RADIANS(`{lon_col_name}`) - RADIANS(%s) ) + SIN( RADIANS(%s) ) * SIN( RADIANS(`{lat_col_name}`) ) ) )"
                    )
                    # The HAVING clause is more appropriate for conditions on calculated aliases.
                    # However, to keep it in WHERE, we repeat the calculation.
                    # For performance, if this is slow, consider a stored function or moving to HAVING if not combined with FTS.
                    condition_sql = f"{distance_calculation_sql} <= %s"
                    conditions.append(condition_sql)
                    params.extend([center_lat, center_lon, center_lat, max_distance_val]) 
                    logger.debug(
                        f"  -> Built DISTANCE condition on columns `{lat_col_name}`, `{lon_col_name}`: "
                        f"center_lat={center_lat}, center_lon={center_lon}, max_dist={max_distance_val}"
                    )
                else:
                    logger.warning(f"Incomplete data or missing lat/lon column names for distance filter on key '{filter_key_config_name}'. Skipping. "
                                   f"LatCol: {lat_col_name}, LonCol: {lon_col_name}, Data: {filter_data}")
            
            elif "values" in filter_data:  # This corresponds to "in" filter type
                value_list = filter_data["values"]
                if value_list: # Ensure list is not empty
                    placeholders = ", ".join(["%s"] * len(value_list))
                    condition_sql = f"`{db_column_name}` IN ({placeholders})"
                    conditions.append(condition_sql)
                    params.extend(value_list)
                    logger.debug(
                        f"  -> Built IN condition for `{db_column_name}`: {condition_sql}, Params added: {value_list}"
                    )
                else:
                    logger.debug(
                        f"  -> Skipped IN condition for column '{db_column_name}' due to empty value list."
                    )

            elif "min" in filter_data and "max" in filter_data: # Range filter with both min and max
                min_val = filter_data["min"]
                max_val = filter_data["max"]
                conditions.append(f"`{db_column_name}` >= %s")
                params.append(min_val)
                conditions.append(f"`{db_column_name}` <= %s")
                params.append(max_val)
                logger.debug(
                    f"  -> Built RANGE condition for `{db_column_name}`: >= {min_val} AND <= {max_val}. Params added: [{min_val}, {max_val}]"
                )
            elif "min" in filter_data:  # Range filter with only min
                min_val = filter_data["min"]
                condition_sql = f"`{db_column_name}` >= %s"
                conditions.append(condition_sql)
                params.append(min_val)
                logger.debug(
                    f"  -> Built MIN_ONLY range condition for `{db_column_name}`: {condition_sql}, Param added: {min_val}"
                )
            elif "max" in filter_data:  # Range filter with only max
                max_val = filter_data["max"]
                condition_sql = f"`{db_column_name}` <= %s"
                conditions.append(condition_sql)
                params.append(max_val)
                logger.debug(
                    f"  -> Built MAX_ONLY range condition for `{db_column_name}`: {condition_sql}, Param added: {max_val}"
                )
            elif "exact" in filter_data: # Numeric/Date exact match (from range parser for single value)
                exact_val = filter_data["exact"]
                condition_sql = f"`{db_column_name}` = %s"
                conditions.append(condition_sql)
                params.append(exact_val)
                logger.debug(
                    f"  -> Built EXACT condition for `{db_column_name}`: {condition_sql}, Param added: {exact_val}"
                )
            elif "value" in filter_data: # String/Enum exact match or 'like'
                val = filter_data["value"]
                if filter_type == "like":
                    condition_sql = f"`{db_column_name}` LIKE %s"
                    params.append(f"%{val}%") # Add wildcards for LIKE
                    logger.debug(
                        f"  -> Built LIKE condition for `{db_column_name}`: {condition_sql}, Param added: %{val}%"
                    )
                else: # exact
                    condition_sql = f"`{db_column_name}` = %s"
                    params.append(val)
                    logger.debug(
                        f"  -> Built VALUE (exact) condition for `{db_column_name}`: {condition_sql}, Param added: {val}"
                    )
            else:
                logger.warning(
                    f"Unknown or empty filter data structure in 'filter_data' for filter key '{filter_key_config_name}' with filter_type '{filter_type}': {filter_data}. Skipping."
                )
        
        final_conditions_sql = " AND ".join(conditions)
        logger.debug(
            f"Finished building filter conditions. SQL: '{final_conditions_sql}', Params: {params}"
        )
        return final_conditions_sql, params 

    def get_filtered_ids(self, table_name: str, filters: Dict[str, Any]) -> List[int]:
        """
        Retrieves IDs from a specified table based on filter conditions.
        This method does not apply a limit; it gets all matching IDs.

        Args:
            table_name (str): The name of the table to fetch data from.
            filters (Dict[str, Any]): Filters to apply to the query, from FilterHandler.

        Returns:
            List[int]: A list of IDs matching the filter conditions.
        """
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for get_filtered_ids: {table_name}")
            return []

        filter_sql, filter_params = self._build_filter_conditions(filters)
        query = f"SELECT id FROM `{table_name}`" # Use backticks
        if filter_sql:
            query += f" WHERE {filter_sql}"
        
        # No LIMIT applied here, as this function is for getting all matching IDs for FAISS filtering
        
        logger.debug(
            f"Executing filtered ID query (no limit): {query} with params: {tuple(filter_params) if filter_params else 'None'}"
        )
        results = self.execute_query(
            query, tuple(filter_params) if filter_params else None
        )

        if results and isinstance(results, list):
            logger.info(f"Filtered ID query returned {len(results)} IDs.")
            return [row["id"] for row in results if "id" in row]
        else:
            logger.warning(
                "Filtered ID query returned no results or results are not in expected format."
            )
            return []

    def get_all_with_filters(
        self, table_name: str, filters: Dict[str, Any], top: int
    ) -> List[int]:
        """
        Retrieve IDs from a table, applying filters and a limit.

        Args:
            table_name: The name of the table.
            filters: A dictionary of parsed filter criteria from FilterHandler.parse_filters.
            top: Maximum number of results to return.

        Returns:
            A list of IDs that match the filter criteria.
        """
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name for get_all_with_filters: {table_name}")
            return []

        logger.debug(
            f"Getting all with filters for table '{table_name}', filters: {filters}, top: {top}"
        )

        filter_sql, filter_params = self._build_filter_conditions(filters)

        query = (
            f"SELECT id FROM `{table_name}`"
        )

        if filter_sql:
            query += f" WHERE {filter_sql}"

        # Append ORDER BY if you want consistent results, e.g., ORDER BY id
        # query += " ORDER BY id" # Consider adding if consistent order is needed

        query += " LIMIT %s"
        final_params = filter_params + [top]

        logger.debug(
            f"Executing get_all_with_filters query: {query} with params: {final_params}"
        )
        results = self.execute_query(query, tuple(final_params))

        if results and isinstance(results, list):
            logger.info(f"get_all_with_filters returned {len(results)} IDs.")
            return [row["id"] for row in results if "id" in row]
        else:
            logger.warning(
                "get_all_with_filters returned no results or results are not in expected format."
            )
            return []