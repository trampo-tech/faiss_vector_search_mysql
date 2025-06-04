import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger()


class DatabaseConnector:
    """
    This isnt particularly safe but well...
    Can likely exploit with SQL injection
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
                database=self.database,
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
        cursor = self.connection.cursor(dictionary=True)  # type: ignore
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
            logger.error(
                f"Error executing query: {e}\nQuery: {query}\nParams: {params}"
            )
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
        if not (table_name.replace("_", "").isalnum()):
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
        if not (table_name.replace("_", "").isalnum()):
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
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name: {table_name}")
            return []

        placeholders = ",".join(["%s"] * len(ids))
        query = f"SELECT * FROM {table_name} WHERE id IN ({placeholders})"
        return self.execute_query(query, tuple(ids)) or []  # type: ignore

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
            logger.warning(f"Invalid table name: {table_name}")
            return []
        if not search_columns:
            logger.warning("Search columns cannot be empty for full-text search.")
            return []

        columns_str = ", ".join(search_columns)

        # For short queries or single characters, use Boolean mode with wildcard
        if len(query_text.strip()) <= 3:
            logger.info(
                f"Short query detected. Using Boolean mode with wildcard for query: {query_text}"
            )
            # Escape special characters and add wildcard for prefix matching
            escaped_query = (
                query_text.replace("+", "\\+")
                .replace("-", "\\-")
                .replace("(", "\\(")
                .replace(")", "\\)")
            )
            search_query = f"{escaped_query}*"
            sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN BOOLEAN MODE) LIMIT %s"
        else:
            logger.info(
                f"Long query detected. Using Natural Language mode for query: {query_text}"
            )
            # Use natural language mode for longer queries
            search_query = query_text
            sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN NATURAL LANGUAGE MODE) LIMIT %s"

        logger.info(
            f"Executing full-text search query: {sql_query} with parameters: {search_query}, {top_n}"
        )
        results = self.execute_query(sql_query, (search_query, top_n))

        if results and isinstance(results, list):
            logger.info(f"Search returned {len(results)} results.")
        else:
            logger.warning(
                "Search returned no results or results are not in expected format."
            )

        return [row["id"] for row in results] if results else []  # type: ignore

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
            List[int]: A list of IDs of the matching documents, ordered by relevance.
        """
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name: {table_name}")
            return []
        if not search_columns:
            logger.warning("Search columns cannot be empty for full-text search.")
            return []

        columns_str = ", ".join(search_columns)

        # For short queries or single characters, use Boolean mode with wildcard
        if len(query_text.strip()) <= 3:
            logger.info(
                f"Short query detected. Using Boolean mode with wildcard for query: {query_text}"
            )
            # Escape special characters and add wildcard for prefix matching
            escaped_query = (
                query_text.replace("+", "\\+")
                .replace("-", "\\-")
                .replace("(", "\\(")
                .replace(")", "\\)")
            )
            search_query = f"{escaped_query}*"
            filter_sql, filter_params = self._build_filter_conditions(filters)
            sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN BOOLEAN MODE)"
            if filter_sql:
                sql_query += f" AND {filter_sql}"
            sql_query += " LIMIT %s"
            params = [search_query] + filter_params + [top_n]
        else:
            logger.info(
                f"Long query detected. Using Natural Language mode for query: {query_text}"
            )
            # Use natural language mode for longer queries
            search_query = query_text
            filter_sql, filter_params = self._build_filter_conditions(filters)
            sql_query = f"SELECT id FROM {table_name} WHERE MATCH({columns_str}) AGAINST (%s IN NATURAL LANGUAGE MODE)"
            if filter_sql:
                sql_query += f" AND {filter_sql}"
            sql_query += " LIMIT %s"
            params = [search_query] + filter_params + [top_n]
        logger.info(
            f"Executing full-text search query with filters: {sql_query} with parameters: {params}"
        )
        results = self.execute_query(sql_query, tuple(params))

        if results and isinstance(results, list):
            logger.info(f"Search returned {len(results)} results.")
        else:
            logger.warning(
                "Search returned no results or results are not in expected format."
            )

        return [row["id"] for row in results] if results else []  # type: ignore

    def _build_filter_conditions(self, filters: Dict[str, Any]) -> Tuple[str, list]:
        logger.debug(f"Building filter conditions for: {filters}")
        conditions = []
        params = []
        for column, filter_detail in filters.items():
            logger.debug(f"Processing filter for column '{column}': {filter_detail}")
            if not isinstance(
                filter_detail, dict
            ):  # Should always be a dict from FilterHandler
                logger.warning(
                    f"Unexpected filter_detail format for column '{column}': {filter_detail}. Skipping."
                )
                continue

            if "values" in filter_detail:
                value_list = filter_detail["values"]
                if value_list:
                    placeholders = ", ".join(["%s"] * len(value_list))
                    condition_sql = f"{column} IN ({placeholders})"
                    conditions.append(condition_sql)
                    params.extend(value_list)
                    logger.debug(
                        f"  -> Built IN condition: {condition_sql}, Params added: {value_list}"
                    )
                else:
                    logger.debug(
                        f"  -> Skipped IN condition for column '{column}' due to empty value list."
                    )

            # Range handling (covers min_only, max_only, min_and_max)
            elif "min" in filter_detail and "max" in filter_detail:
                min_val = filter_detail["min"]
                max_val = filter_detail["max"]
                # Condition for min
                conditions.append(f"{column} >= %s")
                params.append(min_val)
                # Condition for max
                conditions.append(f"{column} <= %s")
                params.append(max_val)
                logger.debug(
                    f"  -> Built RANGE condition: {column} >= {min_val} AND {column} <= {max_val}. Params added: [{min_val}, {max_val}]"
                )
            elif "min" in filter_detail:  # Only min is present
                min_val = filter_detail["min"]
                condition_sql = f"{column} >= %s"
                conditions.append(condition_sql)
                params.append(min_val)
                logger.debug(
                    f"  -> Built MIN_ONLY range condition: {condition_sql}, Param added: {min_val}"
                )
            elif "max" in filter_detail:  # Only max is present
                max_val = filter_detail["max"]
                condition_sql = f"{column} <= %s"
                conditions.append(condition_sql)
                params.append(max_val)
                logger.debug(
                    f"  -> Built MAX_ONLY range condition: {condition_sql}, Param added: {max_val}"
                )

            # Exact match for a field that might otherwise be a range, or specific "exact" type
            elif "exact" in filter_detail:
                exact_val = filter_detail["exact"]
                condition_sql = f"{column} = %s"
                conditions.append(condition_sql)
                params.append(exact_val)
                logger.debug(
                    f"  -> Built EXACT condition: {condition_sql}, Param added: {exact_val}"
                )

            # For string/enum exact matches (typically filter_type "exact" or "like" from FilterConfig)
            elif "value" in filter_detail:
                val = filter_detail["value"]
                condition_sql = f"{column} = %s"  # Assuming '=' for "value" key based on previous setup
                # If "like" is intended, this would need to be column LIKE %s
                conditions.append(condition_sql)
                params.append(val)
                logger.debug(
                    f"  -> Built VALUE condition (assuming '='): {condition_sql}, Param added: {val}"
                )
            else:
                # This case handles if filter_detail was an empty dict or an unrecognized structure.
                logger.warning(
                    f"Unknown or empty filter structure for column '{column}': {filter_detail}. Skipping."
                )

        final_conditions_sql = " AND ".join(conditions)
        logger.debug(
            f"Finished building filter conditions. SQL: '{final_conditions_sql}', Params: {params}"
        )
        return final_conditions_sql, params

    def get_filtered_ids(self, table_name: str, filters: Dict[str, Any]) -> List[int]:
        """
        Retrieves IDs from a specified table based on filter conditions.

        Args:
            table_name (str): The name of the table to fetch data from.
            filters (Dict[str, Any): Filters to apply to the query.

        Returns:
            List[int]: A list of IDs matching the filter conditions.
        """
        if not table_name or not (table_name.replace("_", "").isalnum()):
            logger.warning(f"Invalid table name: {table_name}")
            return []

        filter_sql, filter_params = self._build_filter_conditions(filters)
        query = f"SELECT id FROM {table_name}"
        if filter_sql:
            query += f" WHERE {filter_sql}"
        logger.info(
            f"Executing filtered ID query: {query} with params: {filter_params}"
        )
        results = self.execute_query(
            query, tuple(filter_params) if filter_params else None
        )
        if results and isinstance(results, list):
            logger.info(f"Filtered ID query returned {len(results)} results.")
        else:
            logger.warning(
                "Filtered ID query returned no results or results are not in expected format."
            )

        return [row["id"] for row in results] if results else []  # type: ignore

    def get_all_with_filters(
        self, table_name: str, filters: Dict[str, Any], top: int
    ) -> List[int]:
        """
        Retrieve IDs from a table, applying filters and a limit, consistent with _build_filter_conditions.

        Args:
            table_name: The name of the table.
            filters: A dictionary of parsed filter criteria from FilterHandler.parse_filters.
                     Example: {"status": {"value": "active"}, "price": {"min": 10, "max": 50}}
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

        # Reuse the consistent logic from _build_filter_conditions
        filter_sql, filter_params = self._build_filter_conditions(filters)

        query = (
            f"SELECT id FROM `{table_name}`"  # Use backticks for table name for safety
        )

        if filter_sql:
            query += f" WHERE {filter_sql}"

        # Append ORDER BY if you want consistent results, e.g., ORDER BY id
        # query += " ORDER BY id"

        query += " LIMIT %s"

        # params list for execute_query will be filter_params + [top]
        final_params = filter_params + [top]

        logger.info(
            f"Executing get_all_with_filters query: {query} with params: {final_params}"
        )
        results = self.execute_query(query, tuple(final_params))

        if results and isinstance(results, list):
            logger.info(f"get_all_with_filters returned {len(results)} results.")
            return [row["id"] for row in results]  # type: ignore # Assuming 'id' is the column name
        else:
            logger.warning(
                "get_all_with_filters returned no results or results are not in expected format."
            )
            return []
