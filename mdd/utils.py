import logging
import inspect
import re
import uuid
from itertools import chain
from functools import wraps, reduce
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, TimestampType, BooleanType
from pyspark.sql.functions import create_map, col, lit, expr, row_number

from mdd.environment import Environment

class DecoratorUtil:
    @staticmethod
    def add_logger():
        """Class decorator to add a logger to the class."""
        def decorator(cls):
            cls.logger = logging.getLogger(f"mdd.{cls.__name__}")
            return cls
        return decorator

    @staticmethod
    def _get_clean_call_trace(self_obj, target_func: str, max_depth=10) -> str:
        """
        Returns a clean call trace (e.g., foo -> bar), showing only methods of the same instance
        and excluding the decorator wrapper itself.

        Args:
            self_obj: Instance (`self`) of the current class
            target_func: The actual function being decorated
            max_depth: Stack depth

        Returns:
            str: Clean trace string
        """
        trace = []
        stack = inspect.stack()

        for frame in stack[1:max_depth + 1]:
            frame_self = frame.frame.f_locals.get("self")
            func_name = frame.function

            # Keep only frames from this object
            if frame_self is self_obj:
                # Exclude decorator's internal `wrapper` frame
                if func_name != "wrapper":
                    trace.append(func_name)

        # Add the actual decorated function name at the end
        if not trace or trace[-1] != target_func:
            trace.append(target_func)

        return " -> ".join(trace) if trace else target_func

    @staticmethod
    def log_function(debug_attr: str = "debug"):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if getattr(self, debug_attr, False):
                    trace = DecoratorUtil._get_clean_call_trace(self_obj=self, target_func=func.__name__)
                    self.logger.debug(f"function start: {trace}")
                    result = func(self, *args, **kwargs)
                    self.logger.debug(f"function end: {trace}")
                    return result
                return func(self, *args, **kwargs)
            return wrapper
        return decorator

class FunctionUtil:
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


class DeltaTableUtil:
    @staticmethod
    def get_table_full_name(table_name: str) -> str:
        """
        Ensures the table_name includes the lakehouse name. Adds Environment.catalog_default if missing.

        Args:
            table_name (str): Input table name, possibly partially qualified.

        Returns:
            str: Fully qualified table name.
        """
        parts = table_name.split(".")

        if len(parts) == 3:
            # Already fully qualified: lakehouse.schema.table
            return table_name
        elif len(parts) == 2:
            # Missing lakehouse: add default
            return f"{Environment.catalog_default}.{table_name}"
        elif len(parts) == 1:
            # Only table name provided: assume default schema "default"
            return f"{Environment.catalog_default}.default.{table_name}"
        else:
            raise ValueError(f"Invalid table name format: {table_name}")

    @staticmethod
    def get_max_column_value(
        spark: SparkSession,
        table_name: str,
        target_column: str,
        filters: str = None  # e.g., "region = 'US' AND status = 'active'"
    ):
        """
        Returns the maximum value of a column from a Delta table, optionally filtered by an expression.

        :param spark: SparkSession object
        :param table_name: Full Delta table name (e.g., 'my_db.my_table')
        :param target_column: Column to compute max value for
        :param filters: Optional SQL expression string for filtering rows
        :return: The maximum value of the column, or None if the table is empty or no matches
        :raises ValueError: If the target_column does not exist in the table
        """
        try:
            table_full_name = DeltaTableUtil.get_table_full_name(table_name)
            df = spark.read.format("delta").table(table_full_name)

            if target_column not in df.columns:
                msg = f"Column '{target_column}' not found in table '{table_full_name}'."
                raise ValueError(msg)

            if filters:
                df = df.filter(expr(filters))

            result = df.selectExpr(f"max({target_column}) as max_val").collect()
            return result[0]["max_val"] if result else None

        except Exception as e:
            raise

    @staticmethod
    def safe_drop_columns(df, cols_to_drop):
        existing_cols = [col for col in cols_to_drop if col in df.columns]
        return df.drop(*existing_cols)

    @staticmethod
    def combine_columns_as_dict(df: DataFrame, columns: list[str], new_col_name: str = "combined") -> DataFrame:
        """
        Combine specified columns into a dictionary column where keys are column names and values are row values.

        :param df: Input Spark DataFrame
        :param columns: List of column names to include in the dictionary
        :param new_col_name: Name of the resulting dictionary column (default is 'combined')
        :return: DataFrame with an added MapType column
        """
        kv_pairs = list(chain.from_iterable((lit(c), col(c)) for c in columns))
        return df.withColumn(new_col_name, create_map(*kv_pairs))

    @staticmethod
    def ensure_system_columns(
        spark: SparkSession,
        table_name: str,
        extra_columns: dict = None 
    ):
        """
        Ensures that the specified Delta table has the required system columns.

        Args:
            spark (SparkSession): Spark session object.
            table_name (str): Qualified Delta table name (e.g. 'schema.table').
            corrupt_column (str): Name of the corrupt record column. Defaults to '_corrupt_record'.
            rescued_column (str): Name of the rescued data column. Defaults to '_rescued_data'.

        Returns:
            bool: True if the table has the required system columns, False otherwise.
        """

        # ensure table existence
        table_full_name = DeltaTableUtil.get_table_full_name(table_name)
        try:
            spark.sql(f"DESCRIBE TABLE {table_full_name}")
        except Exception as e:
           raise ValueError(f"Table '{table_full_name}' does not exist.")
       
        extra_columns = extra_columns or {}
        required_columns = extra_columns | {
            "_source_name": StringType(),
            "_source_timestamp": TimestampType(),
            "_record_id": StringType(),
            "_record_timestamp": TimestampType(),
            "_record_deleted": BooleanType()
        }

        # Get current table schema
        existing_columns = {field.name for field in spark.table(table_full_name).schema.fields}

        # Identify missing columns
        missing_columns = {
            col: dtype for col, dtype in required_columns.items() if col not in existing_columns
        }

        if missing_columns:
            try:
                # Add missing columns via SQL
                for col, dtype in missing_columns.items():
                    ddl = f"ALTER TABLE {table_full_name} ADD COLUMNS ({col} {dtype.simpleString()})"
                    spark.sql(ddl)
            except Exception as e:
                raise Exception(f"Error adding missing columns to table '{table_full_name}': {e}")

    @staticmethod
    def get_join_condition(alias_df: str, alias_table: str, primary_keys: str) -> str:
        """
        Constructs a join condition string between two aliases using primary key columns.
        Accepts primary keys as a comma-separated string and splits it internally.

        :param alias_df: Alias of the DataFrame
        :param alias_table: Alias of the table
        :param primary_keys: Comma-separated primary key column names string
        :return: Join condition string
        """
        # Split primary_keys string into list, trimming whitespace
        keys_list = [k.strip() for k in primary_keys.split(",")]

        conditions = [f"{alias_df}.{k} = {alias_table}.{k}" for k in keys_list]
        return reduce(lambda x, y: f"{x} and {y}", conditions)

    @staticmethod
    def get_generated_columns(spark: SparkSession, table_name: str) -> list:
        """
        Extracts a list of generated columns from the given Delta table.

        :param spark: SparkSession object
        :param table_name: QualifiedName of the Delta table
        :return: List of column names that are generated always as
        """
        table_full_name = DeltaTableUtil.get_table_full_name(table_name)
        ddl = spark.sql(f"SHOW CREATE TABLE {table_full_name}").collect()[0][0]
        gen_col_pattern = re.compile(r"generated\s+always\s+as", re.IGNORECASE)
        generated_columns = []

        for line in ddl.splitlines():
            if gen_col_pattern.search(line):
                col_name = line.strip().split()[0].strip("`")
                generated_columns.append(col_name)

        return generated_columns

    @staticmethod
    def match_columns(spark: SparkSession, df: DataFrame, sink_name: str) -> DataFrame:
        """
        Aligns, casts, and validates the DataFrame's columns against the target Delta table,
        excluding generated columns. Reorders columns and attempts to cast them to match
        the target schema.

        :param df: Source DataFrame
        :param sink_name: Fully qualified Delta table name (e.g., 'db.bronze.table')
        :return: DataFrame with aligned and casted columns
        :raises ValueError: If columns do not match or cannot be cast
        """
        # import again since the function is executed in a write stream
        from pyspark.sql import functions as F

        source_columns = set(df.columns)
        target_df = spark.table(sink_name)

        # Get generated columns to exclude from comparison
        generated_columns = DeltaTableUtil.get_generated_columns(spark, sink_name)

        # Build target schema excluding generated columns
        target_schema = [
            field
            for field in target_df.schema.fields
            if field.name not in generated_columns
        ]
        target_column_names = [field.name for field in target_schema]
        target_column_types = {field.name: field.dataType for field in target_schema}

        # Validate source and target columns (as sets, ignoring order)
        if set(target_column_names) != source_columns:
            error_message = (
                f"Schema mismatch for table '{sink_name}'.\n"
                f"Expected columns (excluding generated): {sorted(target_column_names)}\n"
                f"Provided DataFrame columns: {sorted(source_columns)}"
            )
            raise ValueError(error_message)

        # Attempt to cast source DataFrame columns to match target schema
        casted_columns = []
        for col_name in target_column_names:
            target_type: DataType = target_column_types[col_name]
            try:
                casted_columns.append(F.col(col_name).cast(target_type).alias(col_name))
            except Exception as e:
                error_message = f"Failed to cast column '{col_name}' to type '{target_type.simpleString()}': {e}"
                raise ValueError(error_message)

        # Return reordered and casted DataFrame
        return df.select(casted_columns)

    @staticmethod
    def validate_columns(spark: SparkSession, df: DataFrame, sink_name: str):
        """
        Ensure all source columns in the DataFrame exist in the sink table schema,
        excluding any generated columns.

        :param df: DataFrame to validate
        :param sink_name: Fully qualified Delta table name
        :raises ValueError: If any source column does not exist in sink
        """
        source_columns = set(df.columns)
        sink_df = spark.table(sink_name)
        generated_columns = DeltaTableUtil.get_generated_columns(spark, sink_name)
        sink_columns = {
            field.name
            for field in sink_df.schema.fields
            if field.name not in generated_columns
        }

        missing_columns = source_columns - sink_columns
        if missing_columns:
            error_message = (
                f"Update mode validation failed: Source DataFrame contains columns not present in sink table '{sink_name}'.\n"
                f"Missing in sink: {sorted(missing_columns)}\n"
                f"Sink columns (excluding generated): {sorted(sink_columns)}"
            )
            raise ValueError(error_message)

    @staticmethod
    def get_checkpointlocation_path(sink_name: str, checkpointlocation: str, sync_mode: str) -> str:
        """
        Constructs the full checkpoint location path for a given sink.

        :param sink_name: The name of the sink table.
        :param checkpointlocation: Optional suffix for distinguishing multiple checkpoints.
        :param sync_mode: The sync mode ("full", "incremental", etc.) — not used here but included for context or future use.
        :return: Fully qualified checkpoint location path.
        """
        base_path = f"{Environment.root_path_autoloader}/{sink_name}/_checkpoint"

        if checkpointlocation:
            return f"{base_path}/{checkpointlocation}"

        return base_path

    @staticmethod
    def deduplicate(df: DataFrame, primary_key: str, deduplication_columns: str) -> DataFrame:
        """
        Deduplicates the DataFrame by keeping only one record per primary key,
        using composite deduplication columns with optional sort direction.

        :param df: Input Spark DataFrame
        :param primary_key: Comma-separated list of primary key columns (e.g., "id,sub_id")
        :param deduplication_columns: String defining columns and sort directions
            (e.g., "event_time desc, event_sequence", defaults to asc if not specified)
        :return: Deduplicated DataFrame
        """
        if not primary_key or not deduplication_columns:
            message = "primary key and deduplication columns are required"
            raise ValueError(message)

        primary_keys = FunctionUtil.string_to_list(primary_key)
        try:
            # Parse deduplication column string
            sort_exprs = []
            for entry in deduplication_columns.split(","):
                parts = entry.strip().split()
                if len(parts) == 1:
                    col_name, direction = parts[0], "asc"
                elif len(parts) == 2:
                    col_name, direction = parts[0], parts[1].lower()
                else:
                    raise ValueError(
                        f"Invalid format for deduplication column: '{entry.strip()}'"
                    )

                if direction == "desc":
                    sort_exprs.append(col(col_name).desc())
                elif direction == "asc":
                    sort_exprs.append(col(col_name).asc())
                else:
                    raise ValueError(
                        f"Unsupported sort order '{direction}' for column '{col_name}'"
                    )

            window_spec = Window.partitionBy(*primary_keys).orderBy(*sort_exprs)
            df_deduplicated = (
                df.withColumn("_row_number", row_number().over(window_spec))
                .filter(col("_row_number") == 1)
                .drop("_row_number")
            )

        except Exception as e:
            message = f"Error during deduplication: {e}"
            raise Exception(message)

        return df_deduplicated
