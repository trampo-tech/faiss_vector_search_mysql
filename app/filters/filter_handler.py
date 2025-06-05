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
        Expected format: "status:disponivel,preco_diario:10-50,categoria_id:1,2,3"
        The returned structure for each filter will be:
        {
            "column_name": {
                "filter_data": { ... parsed value like {"values": [...]} or {"min": ..., "max": ...} ... },
                "data_type": "string" | "int" | "decimal" | "date" | "enum",
                "filter_type": "exact" | "in" | "range" | "like"
            }
        }
        """
        if not filters_param or not table_config.filters:
            return {}

        parsed_filters = {}
        filter_pairs = filters_param.split(",")

        available_filters = {f.column: f for f in table_config.filters}

        for pair in filter_pairs:
            if ":" not in pair:
                continue

            column, value_str = pair.split(":", 1)
            column = column.strip()
            # Do not lowercase column name
            # Lowercase only the value part of the filter
            value_str = value_str.strip().lower() # Lowercase the value string here

            if column not in available_filters:
                logger.warning(f"Filter column '{column}' not configured for table")
                continue

            filter_config = available_filters[column]
            # Pass the already lowercased value_str to _parse_filter_value
            parsed_value_data = FilterHandler._parse_filter_value(value_str, filter_config)

            if parsed_value_data is not None:
                parsed_filters[column] = {
                    "filter_data": parsed_value_data,
                    "data_type": filter_config.data_type,
                    "filter_type": filter_config.filter_type,
                }
        logger.debug(f"Got the following parsed filters:{parsed_filters}")
        return parsed_filters

    @staticmethod
    def _parse_filter_value(value: str, filter_config: FilterConfig) -> Any:
        # value parameter is already lowercased by the caller (parse_filters)
        logger.debug(
            f"Parsing filter value '{value}' for column '{filter_config.column}' with type '{filter_config.filter_type}' and data_type '{filter_config.data_type}'"
        )
        try:
            if filter_config.filter_type == "range":
                # ... (your existing improved range logic) ...
                # value is already lowercased, but range parsing deals with numbers or dates mostly
                value_str = value # Already lowercase
                parsed_range = {}
                if "-" in value_str:
                    parts = value_str.split("-", 1)
                    min_part, max_part = parts[0].strip(), parts[1].strip()
                    if min_part:
                        try:
                            parsed_range["min"] = FilterHandler._convert_value(
                                min_part, filter_config.data_type
                            )
                        except ValueError:
                            return None
                    if max_part:
                        try:
                            parsed_range["max"] = FilterHandler._convert_value(
                                max_part, filter_config.data_type
                            )
                        except ValueError:
                            return None
                    return parsed_range if parsed_range else None
                else:
                    try:
                        return {
                            "exact": FilterHandler._convert_value(
                                value_str, filter_config.data_type
                            )
                        }
                    except ValueError:
                        return None

            elif filter_config.filter_type == "in":
                # value is already lowercased. Split parts are inherently lowercase.
                raw_values = [v.strip() for v in value.split(" ")]
                parsed_and_validated_values = []

                for v_str in raw_values: # v_str is already lowercase
                    is_valid_for_enum = True
                    if (
                        filter_config.data_type == "enum"
                        and filter_config.valid_enum_values
                    ):
                        # Compare lowercase v_str with lowercase valid_enum_values
                        lowercase_valid_enum_values = [
                            ve.lower() for ve in filter_config.valid_enum_values
                        ]
                        if v_str not in lowercase_valid_enum_values:
                            logger.warning(
                                f"Value '{v_str}' in 'IN' clause for enum column '{filter_config.column}' "
                                f"is not in its configured valid_enum_values ({filter_config.valid_enum_values}) "
                                f"for table. Excluding this value."
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

                if (
                    not parsed_and_validated_values
                ):  # If all values were invalid or list became empty
                    logger.warning(
                        f"All values in 'IN' clause for column '{filter_config.column}' were invalid or list is empty. Ignoring filter."
                    )
                    return None
                logger.debug(
                    f"Parsed 'in' values for '{filter_config.column}': {parsed_and_validated_values}"
                )
                return {"values": parsed_and_validated_values}

            elif filter_config.filter_type in [
                "exact",
                "like",
            ]:  # "like" is unusual for strict enums
                val_str = value # value is already lowercased

                if (
                    filter_config.data_type == "enum"
                    and filter_config.valid_enum_values
                ):
                    # Compare lowercase val_str with lowercase valid_enum_values
                    lowercase_valid_enum_values = [
                        ve.lower() for ve in filter_config.valid_enum_values
                    ]
                    if val_str not in lowercase_valid_enum_values:
                        logger.warning(
                            f"Value '{val_str}' for enum column '{filter_config.column}' is not in its "
                            f"configured valid_enum_values ({filter_config.valid_enum_values}) for table. "
                            f"Ignoring this filter component for this table."
                        )
                        return None  # This signals to parse_filters to skip this specific filter.

                # If not an enum with validation, or if it's a valid enum value, proceed
                try:
                    parsed_val = FilterHandler._convert_value(
                        val_str, filter_config.data_type
                    )
                    logger.debug(
                        f"Parsed 'exact' (or 'like') value for '{filter_config.column}': {parsed_val}"
                    )
                    return {
                        "value": parsed_val
                    }  # For "like", SQL side would need to handle wildcards
                except ValueError:
                    logger.warning(
                        f"Could not convert value '{val_str}' for column '{filter_config.column}'. Ignoring filter."
                    )
                    return None

        except Exception as e:
            logger.error(
                f"Failed to parse filter value '{value}' for column '{filter_config.column}': {e}",
                exc_info=True,
            )
            return None
        # Fallback if no condition matched (should be covered by specific type logic)
        logger.warning(
            f"Filter value '{value}' for column '{filter_config.column}' did not match any parsing logic."
        )
        return None

    @staticmethod
    def _convert_value(value: str, data_type: str) -> Any:
        """Convert string value to appropriate data type. Can raise ValueError."""
        # value parameter is already lowercased by _parse_filter_value,
        # which received it lowercased from parse_filters.
        # This is fine for string/enum. For numeric/date, case doesn't matter.
        logger.debug(f"Converting value '{value}' to data_type '{data_type}'")
        if not value and data_type not in [
            "string",
            "enum",
        ]:  # Allow empty strings for string/enum, but not for int/decimal/date
            raise ValueError(f"Empty value cannot be converted to {data_type}")
        if data_type == "int":
            return int(value)
        elif data_type == "decimal":
            return float(value)
        elif data_type == "date":
            return datetime.fromisoformat(value.replace("T", " "))
        else:  # string, enum
            return value
