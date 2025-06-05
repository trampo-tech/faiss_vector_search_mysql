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
        """
        if not filters_param or not table_config.filters:
            return {}

        parsed_filters = {}
        filter_pairs = filters_param.split(",")

        available_filters = {f.column: f for f in table_config.filters}

        for pair in filter_pairs:
            if ":" not in pair:
                continue

            column, value = pair.split(":", 1)
            column = column.strip()
            value = value.strip()

            if column not in available_filters:
                logger.warning(f"Filter column '{column}' not configured for table")
                continue

            filter_config = available_filters[column]
            parsed_value = FilterHandler._parse_filter_value(value, filter_config)

            if parsed_value is not None:
                parsed_filters[column] = parsed_value
        logger.debug(f"Got the following parsed filters:{parsed_filters}")
        return parsed_filters

    @staticmethod
    def _parse_filter_value(value: str, filter_config: FilterConfig) -> Any:
        logger.debug(
            f"Parsing filter value '{value}' for column '{filter_config.column}' with type '{filter_config.filter_type}' and data_type '{filter_config.data_type}'"
        )
        try:
            if filter_config.filter_type == "range":
                # ... (your existing improved range logic) ...
                # Example for range (ensure it returns None on failure, which is handled by parse_filters)
                value_str = value.strip()
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
                raw_values = [v.strip() for v in value.split(" ")]
                parsed_and_validated_values = []

                for v_str in raw_values:
                    is_valid_for_enum = True
                    if (
                        filter_config.data_type == "enum"
                        and filter_config.valid_enum_values
                    ):
                        if v_str not in filter_config.valid_enum_values:
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
                val_str = value.strip()  # Use value.strip() for single values too

                if (
                    filter_config.data_type == "enum"
                    and filter_config.valid_enum_values
                ):
                    if val_str not in filter_config.valid_enum_values:
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
