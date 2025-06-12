import logging
from datetime import datetime
from typing import Any, Dict

from ..config import FilterConfig, TableConfig

logger = logging.getLogger(__name__)


class FilterHandler:
    @staticmethod
    def parse_filters(filters_param: str, table_config: TableConfig) -> Dict[str, Any]:
        """
        Parse filter string into structured filters.
        Expected format: "status:disponivel;preco_diario:10-50;categoria_id:1,2,3;localizacao:37.7,-122.4,10"
        The returned structure for each filter will be:
        {
            "column_name": {
                "filter_data": { ... parsed value ... },
                "data_type": "string" | "int" | "decimal" | "date" | "enum" | "geo",
                "filter_type": "exact" | "in" | "range" | "like" | "distance",
                "latitude_column_name"?: "lat_col", // Only for distance filter
                "longitude_column_name"?: "lon_col" // Only for distance filter
            }
        }
        """
        if not filters_param or not table_config.filters:
            logger.debug("No filters_param provided or no filters configured for the table.")
            return {}

        parsed_filters: Dict[str, Any] = {}
        # User's current code uses ';' as separator
        filter_pairs = filters_param.split(";")

        available_filters: Dict[str, FilterConfig] = {
            f.column: f for f in table_config.filters
        }

        for pair in filter_pairs:
            if ":" not in pair:
                logger.warning(f"Skipping malformed filter pair (missing ':'): '{pair}'")
                continue

            column, value_str = pair.split(":", 1)
            column = column.strip()
            # Do not lowercase column name
            # Lowercase only the value part of the filter
            value_str = value_str.strip().lower() # Lowercase the value string here

            if column not in available_filters:
                logger.warning(
                    f"Filter column '{column}' not configured for table '{table_config.name}'. Skipping."
                )
                continue

            filter_config = available_filters[column]
            # Pass the already lowercased value_str to _parse_filter_value
            parsed_value_data = FilterHandler._parse_filter_value(
                value_str, filter_config
            )

            if parsed_value_data is not None:
                entry: Dict[str, Any] = {
                    "filter_data": parsed_value_data,
                    "data_type": filter_config.data_type,
                    "filter_type": filter_config.filter_type,
                }
                if filter_config.filter_type == "distance":
                    entry["latitude_column_name"] = table_config.latitude_column
                    entry["longitude_column_name"] = table_config.longitude_column
                
                parsed_filters[column] = entry
            else:
                logger.warning(
                    f"Could not parse value for filter column '{column}' with value '{value_str}'. Filter skipped."
                )

        logger.debug(f"Parsed filters for table '{table_config.name}': {parsed_filters}")
        return parsed_filters

    @staticmethod
    def _parse_filter_value(value: str, filter_config: FilterConfig) -> Any:
        # value parameter is already lowercased by the caller (parse_filters)
        logger.debug(
            f"Parsing filter value '{value}' for column '{filter_config.column}' with type '{filter_config.filter_type}' and data_type '{filter_config.data_type}'"
        )
        try:
            if filter_config.filter_type == "range":
                value_str = value  # Already lowercase
                parsed_range: Dict[str, Any] = {} # Ensure parsed_range is typed
                if "-" in value_str:
                    parts = value_str.split("-", 1)
                    min_part, max_part = parts[0].strip(), parts[1].strip()
                    if min_part: # Check if min_part is not empty
                        try:
                            parsed_range["min"] = FilterHandler._convert_value(
                                min_part, filter_config.data_type
                            )
                        except ValueError:
                            logger.warning(f"Invalid min value for range filter on '{filter_config.column}': {min_part}")
                            return None
                    if max_part: # Check if max_part is not empty
                        try:
                            parsed_range["max"] = FilterHandler._convert_value(
                                max_part, filter_config.data_type
                            )
                        except ValueError:
                            logger.warning(f"Invalid max value for range filter on '{filter_config.column}': {max_part}")
                            return None
                    # Ensure at least one part of the range was successfully parsed
                    return parsed_range if parsed_range else None
                else: # Single value, treat as exact match within range logic if necessary or specific handling
                    try:
                        # This was returning "exact" which might be confusing for a "range" type.
                        # For a range filter, a single value could mean "min=" or "exact=" depending on convention.
                        # Let's assume it means an exact match for the range type if only one value is given.
                        # Or, it could be interpreted as min_val = max_val = value.
                        # For simplicity, let's assume it's an exact value for now.
                        # If you want it to be min=value or max=value, adjust accordingly.
                        exact_val = FilterHandler._convert_value(
                            value_str, filter_config.data_type
                        )
                        return {"exact": exact_val } # Or {"min": exact_val, "max": exact_val}
                    except ValueError:
                        logger.warning(f"Invalid single value for range filter on '{filter_config.column}': {value_str}")
                        return None

            elif filter_config.filter_type == "in":
                raw_values = [v.strip() for v in value.split(",")]
                parsed_and_validated_values = []

                for v_str in raw_values:  # v_str is already lowercase
                    if not v_str: # Skip empty strings resulting from "val1,,val2"
                        continue
                    is_valid_for_enum = True
                    if (
                        filter_config.data_type == "enum"
                        and filter_config.valid_enum_values
                    ):
                        lowercase_valid_enum_values = [
                            ve.lower() for ve in filter_config.valid_enum_values
                        ]
                        if v_str not in lowercase_valid_enum_values:
                            logger.warning(
                                f"Value '{v_str}' in 'IN' clause for enum column '{filter_config.column}' "
                                f"is not in its configured valid_enum_values. Excluding this value."
                            )
                            is_valid_for_enum = False

                    if is_valid_for_enum:
                        try:
                            parsed_and_validated_values.append(
                                FilterHandler._convert_value(
                                    v_str, filter_config.data_type
                                )
                            )
                        except ValueError:
                            logger.warning(
                                f"Could not convert value '{v_str}' for column '{filter_config.column}' in IN list. Skipping value."
                            )

                if not parsed_and_validated_values:
                    logger.warning(
                        f"All values in 'IN' clause for column '{filter_config.column}' were invalid or list is empty. Ignoring filter."
                    )
                    return None
                logger.debug(
                    f"Parsed 'in' values for '{filter_config.column}': {parsed_and_validated_values}"
                )
                return {"values": parsed_and_validated_values}

            elif filter_config.filter_type in ["exact", "like"]:
                val_str = value  # value is already lowercased

                if (
                    filter_config.data_type == "enum"
                    and filter_config.valid_enum_values
                ):
                    lowercase_valid_enum_values = [
                        ve.lower() for ve in filter_config.valid_enum_values
                    ]
                    if val_str not in lowercase_valid_enum_values:
                        logger.warning(
                            f"Value '{val_str}' for enum column '{filter_config.column}' is not in its "
                            f"configured valid_enum_values. Ignoring this filter component."
                        )
                        return None

                try:
                    parsed_val = FilterHandler._convert_value(
                        val_str, filter_config.data_type
                    )
                    logger.debug(
                        f"Parsed 'exact' (or 'like') value for '{filter_config.column}': {parsed_val}"
                    )
                    return {"value": parsed_val}
                except ValueError:
                    logger.warning(
                        f"Could not convert value '{val_str}' for column '{filter_config.column}'. Ignoring filter."
                    )
                    return None
            
            elif filter_config.filter_type == "distance":
                if filter_config.data_type != "geo":
                    logger.warning(
                        f"Distance filter for column '{filter_config.column}' must have data_type 'geo'. "
                        f"Configured: '{filter_config.data_type}'. Skipping."
                    )
                    return None
                
                parts = value.split(',') # value is "lat,lon,dist_km_or_miles"
                if len(parts) != 3:
                    logger.warning(
                        f"Distance filter value '{value}' for column '{filter_config.column}' "
                        f"must be in 'latitude,longitude,distance' format. Skipping."
                    )
                    return None
                try:
                    center_lat = float(parts[0].strip())
                    center_lon = float(parts[1].strip())
                    max_dist = float(parts[2].strip()) # Unit (km/miles) depends on DB query
                    
                    if not (-90 <= center_lat <= 90 and -180 <= center_lon <= 180):
                        logger.warning(
                            f"Invalid latitude/longitude values in distance filter '{value}' for column '{filter_config.column}'. Skipping."
                        )
                        return None

                    if max_dist <= 0:
                        logger.warning(
                            f"Distance filter max_distance '{max_dist}' for column '{filter_config.column}' must be positive. Skipping."
                        )
                        return None
                    
                    logger.debug(
                        f"Parsed distance filter for '{filter_config.column}': lat={center_lat}, lon={center_lon}, dist={max_dist}"
                    )
                    # The key for distance can be generic like "max_distance" and the unit (km/miles)
                    # should be consistently handled by the database query part.
                    return {"center_lat": center_lat, "center_lon": center_lon, "max_distance": max_dist}
                except ValueError:
                    logger.warning(
                        f"Could not parse numeric values from distance filter value '{value}' for column '{filter_config.column}'. Skipping."
                    )
                    return None

        except Exception as e:
            logger.error(
                f"Failed to parse filter value '{value}' for column '{filter_config.column}': {e}",
                exc_info=True,
            )
            return None
        
        logger.warning(
            f"Filter value '{value}' for column '{filter_config.column}' did not match any parsing logic for filter_type '{filter_config.filter_type}'."
        )
        return None

    @staticmethod
    def _convert_value(value: str, data_type: str) -> Any:
        """Convert string value to appropriate data type. Can raise ValueError."""
        logger.debug(f"Converting value '{value}' to data_type '{data_type}'")
        
        # Allow empty strings only if data_type is string or enum and value is indeed empty.
        # For other types, an empty string is usually an invalid input.
        if not value and data_type not in ["string", "enum"]:
            raise ValueError(f"Empty value cannot be converted to {data_type}")

        if data_type == "int":
            return int(value)
        elif data_type == "decimal": # Using float for decimal for simplicity here
            return float(value)
        elif data_type == "date":
            # Attempt to parse ISO format, allow for 'Z' at the end or timezone info
            # For simplicity, this example assumes dates are without timezone or UTC 'Z'
            # and converts 'T' to space for datetime.fromisoformat if needed.
            # A more robust date parser might be needed for varied formats.
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                 # Fallback for formats like "YYYY-MM-DD HH:MM:SS"
                return datetime.fromisoformat(value.replace("T", " "))
        elif data_type == "string" or data_type == "enum":
            return value # Value is already lowercased by caller if it was a string/enum
        else:
            logger.warning(f"Unknown data_type '{data_type}' for value '{value}'. Returning as string.")
            return value # Fallback or raise error