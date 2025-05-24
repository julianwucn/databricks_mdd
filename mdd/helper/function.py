import re
import uuid
from functools import reduce

class FunctionUtils:
    @staticmethod
    def timestamp_to_string(timestamp):
        """
        Convert timestamp to string format: 'YYYY-MM-DD HH:mm:ss.SSSSSS'
        """
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

    @staticmethod
    def get_temp_view_name() -> str:
        """
        Generate a unique temporary view name.
        """
        # "-" is not allowed in spark sql
        return f"vw_temp_{str(uuid.uuid4()).replace('-', '_')}"

    @staticmethod
    def string_to_list(value: str):
        """
        Convert a comma-separated string to a list of cleaned keys.
        """
        if not value:
            return []
        return [k.strip() for k in value.split(",") if k.strip()]
