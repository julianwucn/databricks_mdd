import logging
import inspect
import re
import uuid
import datetime
from itertools import chain
from functools import wraps, reduce
from typing import Optional
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import *
from pyspark.sql.functions import create_map, col, lit, expr, row_number, current_timestamp, desc

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
                    self.logger.debug(f"Function start: {trace}")
                    result = func(self, *args, **kwargs)
                    self.logger.debug(f"Function end: {trace}")
                    return result
                return func(self, *args, **kwargs)
            return wrapper
        return decorator

class NotebookUtil:
    @staticmethod
    def get_notebook_name(spark: SparkSession) -> str:
        """
        Returns the name of the current notebook.
        """
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        notebook_name = notebook_path.split("/")[-1]

        return notebook_name

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
        full_path = f"{Environment.root_path_state}/{sink_name}/_checkpoint"

        # delete the checkpoint location if the mode is full or backfill
        if sync_mode in ["full", "backfill"]:
            shutil.rmtree(full_path, ignore_errors=True)

        if checkpointlocation:
            full_path = f"{full_path}/{checkpointlocation}"

        if sync_mode:
            full_path = f"{full_path}/{sync_mode}"

        return full_path

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
        if not deduplication_columns:
            return df

        if not primary_key:
            message = "primary key is required"
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
    
    @staticmethod
    def columns_exist(df, columns, match_all=True):
        """
        Check if specified columns exist in a DataFrame.

        Parameters:
            df (DataFrame): The PySpark DataFrame.
            columns (List[str]): List of column names to check.
            match_all (bool): If True, checks that all columns exist.
                            If False, checks that at least one exists.

        Returns:
            bool: True if condition met, False otherwise.
        """
        df_columns = set(df.columns)
        target_columns = set(columns)

        if match_all:
            return target_columns.issubset(df_columns)
        else:
            return not df_columns.isdisjoint(target_columns)

class MDDUtil:
    @staticmethod
    def save_processed_version(
        spark: SparkSession,
        table_name: str,
        source_name: str,
        source_commit_version: int,
        data_sync_mode: str
    ):
        # Define schema (matching table definition)
        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("source_commit_version", LongType(), True),
            StructField("data_sync_mode", StringType(), True)
        ])

        # Create a single-row DataFrame
        data = [(table_name, source_name, source_commit_version, data_sync_mode)]
        df = spark.createDataFrame(data, schema=schema).withColumn("log_timestamp", current_timestamp())

        # Append data to the table
        df.write.format("delta").mode("append").saveAsTable("mdd.table_control")

    @staticmethod
    def get_processed_version(
        spark: SparkSession,
        table_name: str,
        source_name: str
    ) -> Optional[int]:
        """
        Returns the source_commit_version corresponding to the latest log_timestamp
        for the given table_name and source_name from mdd.table_control.
        Returns None if no match is found.
        """
        try:
            df = (
                spark.table("mdd.table_control")
                .filter((col("table_name") == table_name) & (col("source_name") == source_name))
                .orderBy(desc("log_timestamp"))
                .select("source_commit_version")
                .limit(1)
            )

            result = df.collect()
            return result[0]["source_commit_version"] if result else None

        except Exception as e:
            print(f"Error accessing mdd.table_control: {e}")
            return None
    
    @staticmethod
    def get_run_id(name: str, timestamp: datetime.datetime) -> str:
        return f"{name}_{FunctionUtil.timestamp_to_string(timestamp)}"

    @staticmethod
    def etl_job_update(
        job_name,
        job_start_timestamp,
        job_end_timestamp,
        job_status,
        error_message
    ):
        table_full_name = DeltaTableUtil.get_table_full_name("mdd.etl_job")
        job_run_id = MDDUtil.get_run_id(job_name, job_start_timestamp)
        log_timestamp = datetime.datetime.utcnow()

        data = [
            (
                job_run_id,
                job_name,
                job_start_timestamp,
                job_end_timestamp,
                job_status,
                error_message,
                log_timestamp
            )
        ]

        spark_session = SparkSession.builder.getOrCreate()
        df = spark_session.createDataFrame(
            data,
            "job_run_id string, job_name string, job_start_timestamp timestamp, job_end_timestamp timestamp, job_status string, error_message string, log_timestamp timestamp",
        )

        dt = DeltaTable.forName(spark_session, table_full_name)
        (
            dt.alias("t")
            .merge(
                df.alias("s"),
                f"s.job_run_id = t.job_run_id and s.job_name = t.job_name and t.job_name = '{job_name}'",
            )
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(
                set={
                    "job_end_timestamp": "s.job_end_timestamp",
                    "job_status": "s.job_status",
                    "error_message": "s.error_message",
                    "log_timestamp": "s.log_timestamp"
                }
            )
            .execute()
        )

    @staticmethod
    def etl_job_tasks_update(
        job_name: str,
        job_start_timestamp: datetime.datetime,
        task_name: str,
        task_start_timestamp: datetime.datetime,
        task_end_timestamp: datetime.datetime,
        task_status: str,
        error_message: str
    ):
        table_full_name = DeltaTableUtil.get_table_full_name("mdd.etl_job_tasks")
        task_run_id = MDDUtil.get_run_id(task_name, task_start_timestamp)
        job_run_id = MDDUtil.get_run_id(job_name, job_start_timestamp)
        log_timestamp = datetime.datetime.utcnow()

        data = [
            (
                task_run_id,
                task_name,
                task_start_timestamp,
                task_end_timestamp,
                task_status,
                job_run_id,
                error_message,
                log_timestamp
            )
        ]
        spark_session = SparkSession.builder.getOrCreate()
        df = spark_session.createDataFrame(
            data,
            "task_run_id string, task_name string, task_start_timestamp timestamp, task_end_timestamp timestamp, task_status string, job_run_id string, error_message string, log_timestamp timestamp",
        )
        dt = DeltaTable.forName(spark_session, table_full_name)
        (
            dt.alias("t")
            .merge(
                df.alias("s"),
                f"s.task_run_id = t.task_run_id and s.task_name = t.task_name and t.task_name = '{task_name}'",
            )
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(
                set={
                    "task_end_timestamp": "s.task_end_timestamp",
                    "task_status": "s.task_status",
                    "error_message": "s.error_message",
                    "log_timestamp": "s.log_timestamp"
                }
            )
            .execute()
        )

    @staticmethod
    def etl_job_files_update(
        file_run_id,
        file_name,
        file_timestamp,
        file_date,
        file_path,
        table_name,
        data_sync_mode,
        data_write_mode,
        rows_read,
        rows_onboarded,
        task_run_id,
        job_run_id
    ):
        table_full_name = DeltaTableUtil.get_table_full_name("mdd.etl_job_tasks")
        log_timestamp = datetime.datetime.utcnow()

        data = [
            (
                file_run_id,
                file_name,
                file_timestamp,
                file_date,
                file_path,
                file_rows,
                table_name,
                data_sync_mode,
                data_write_mode,
                rows_read,
                rows_onboarded,
                task_run_id,
                job_run_id,
                log_timestamp
            )
        ]
        spark_session = SparkSession.builder.getOrCreate()
        df = spark_session.createDataFrame(
            data,
            "file_run_id string, file_name string, file_timestamp timestamp, file_date string, file_path string, table_name string, data_sync_mode string, data_write_mode string, rows_read long, rows_onboarded long, task_run_id string , job_run_id string , log_timestamp timestamp",
        )
        df.write.mode("append").saveAsTable(table_full_name)
        
    @staticmethod
    def etl_job_tables_update(    
    table_run_id              
    ,table_name               
    ,data_sync_mode  
    ,data_write_mode         
    ,source_table_name        
    ,source_table_timestamp   
    ,source_table_cfversion   
    ,rows_read                
    ,batches                  
    ,batch                    
    ,rows_in_batch            
    ,rows_inserted            
    ,rows_updated             
    ,rows_deleted             
    ,read_start_timestamp     
    ,read_end_timestamp       
    ,transform_start_timestamp
    ,transform_end_timestamp  
    ,write_start_timestamp    
    ,write_end_timestamp      
    ,task_run_id              
    ,job_run_id               
    ,log_timestamp            
            
    ):
        table_full_name = DeltaTableUtil.get_table_full_name("mdd.etl_job_tables")
        data = [(    
            table_run_id              
            ,table_name               
            ,data_sync_mode  
            ,data_write_mode         
            ,source_table_name        
            ,source_table_timestamp   
            ,source_table_cfversion   
            ,rows_read                
            ,batches                  
            ,batch                    
            ,rows_in_batch            
            ,rows_inserted            
            ,rows_updated             
            ,rows_deleted             
            ,read_start_timestamp     
            ,read_end_timestamp       
            ,transform_start_timestamp
            ,transform_end_timestamp  
            ,write_start_timestamp    
            ,write_end_timestamp      
            ,task_run_id              
            ,job_run_id               
            ,log_timestamp            
            
        )]

        schema = """
        table_run_id                   string
        ,table_name                    string
        ,data_sync_mode                string
        ,data_write_mode               string
        ,source_table_name             string
        ,source_table_timestamp        timestamp
        ,source_table_cfversion        long
        ,rows_read                     long
        ,batches                       long
        ,batch                         string
        ,rows_in_batch                 long
        ,rows_inserted                 long
        ,rows_updated                  long
        ,rows_deleted                  long
        ,read_start_timestamp          timestamp
        ,read_end_timestamp            timestamp
        ,transform_start_timestamp     timestamp
        ,transform_end_timestamp       timestamp
        ,write_start_timestamp         timestamp
        ,write_end_timestamp           timestamp
        ,task_run_id                   string
        ,job_run_id                    string
        ,log_timestamp                 timestamp
        """

        spark_session = SparkSession.builder.getOrCreate()

        df = spark_session.createDataFrame(data, schema)
        df.write.mode("append").saveAsTable(table_full_name)