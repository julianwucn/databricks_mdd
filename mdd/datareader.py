from pyspark.sql import DataFrame, SparkSession

# the following import moved to the read stream to avoid the error "name 'col' is not defined"
# interactive notebooks will keep the state of last run and the functions will be out of scope
# from pyspark.sql.functions import lit, col, expr, current_timestamp
from typing import Optional, Dict, Tuple
import inspect
import logging
from delta.tables import DeltaTable
from pyspark.sql.functions import row_number, col, to_date, current_timestamp, expr
from pyspark.sql.window import Window

from mdd.utils import DecoratorUtil, DeltaTableUtil
from mdd.environment import Environment

@DecoratorUtil.add_logger()
class FileReader:
    logger: logging.Logger
    def __init__(
        self,
        spark: SparkSession,
        config: Dict,
        debug: bool = False,
    ):
        """
        Initializes the FileReader class with the provided SparkSession, config, and debug flag.
        Parameters:
            spark (SparkSession): The SparkSession object.
            config (Dict): The configuration dictionary.
            debug (bool, optional): Whether to enable debug logging. Defaults to False.
        """
        self.config = config
        self.spark = spark
        self.source_format = config["source_format"]
        self.source_options = config["source_options"] or {}
        self.source_schema = config["source_schema"]
        self.source_path = f"{Environment.root_path_data}/{config["source_relative_path"]}"
        self.sink_name = config["sink_name"]
        self.debug = debug

        if debug:
            self.logger.debug(f"source_path: {self.source_path}")
            self.logger.debug(f"source_format: {self.source_format}")
            self.logger.debug(f"source_schema: {self.source_schema}")
            self.logger.debug(f"source_options: {self.source_options}")
            self.logger.debug(f"sink_name: {self.sink_name}")

        try:
            location_key = "cloudFiles.schemaLocation"
            if location_key not in self.source_options:
                raise ValueError(
                    "{location_key} must be specified in source_options for streaming writes"
                )
            else:
                # rebuild the location
                schema_location = (
                    f"{Environment.root_path_state}/{self.sink_name}/_schema"
                )
                cloudfiles_schemalocation = self.source_options[location_key]
                if cloudfiles_schemalocation is not None:
                    # support mutliple sources
                    schema_location = f"{schema_location}/{cloudfiles_schemalocation}"
                self.source_options[location_key] = schema_location

            if self.debug:
                self.logger.debug(
                    f"{location_key}: {self.source_options[location_key]}"
                )

        except Exception as e:
            self.logger.error(f"Initialization error: {e}")
            raise

    @DecoratorUtil.log_function()
    def read_stream(self) -> DataFrame:
        """
        Sets up and returns a streaming DataFrame using Auto Loader.
        """
        source_schema = self.source_schema

        # add the corrupt column to the schema if not included
        corrupt_column = "_corrupt_record"
        if source_schema:
            corrupt_column_key = "columnNameOfCorruptRecord"
            if (
                corrupt_column_key in self.source_options
                and self.source_options[corrupt_column_key] is not None
                and self.source_options[corrupt_column_key] != ""
            ):
                corrupt_column = self.source_options[corrupt_column_key]
            if corrupt_column not in source_schema:
                source_schema = f"{source_schema},{corrupt_column} string"

        # add the rescue column to the schema if not included
        rescue_column = "_rescue_data"
        if source_schema:
            rescue_column_key = "rescuedDataColumn"
            if (
                rescue_column_key in self.source_options
                and self.source_options[rescue_column_key] is not None
                and self.source_options[rescue_column_key] != ""
            ):
                rescue_column = self.source_options[rescue_column_key]
            if rescue_column not in source_schema:
                source_schema = f"{source_schema}, {rescue_column} string"

        reader = self.spark.readStream.format(self.source_format).options(
            **self.source_options
        )

        if source_schema:
            reader = reader.schema(source_schema)

        if self.source_path:
            reader = reader.load(self.source_path)
        else:
            reader = reader.load()

        from pyspark.sql.functions import lit, col, expr, current_timestamp

        # add corrupt column and rescue column if not included
        if corrupt_column not in reader.columns:
            reader = reader.withColumn(corrupt_column, lit(None).cast("string"))
        if rescue_column not in reader.columns:
            reader = reader.withColumn(rescue_column, lit(None).cast("string"))

        # add the metadata columns
        reader = reader.withColumn("_metadata", col("_metadata"))

        if self.debug:
            self.logger.debug(f"reader schema: {reader.schema.simpleString()}")

        return reader


@DecoratorUtil.add_logger()
class DeltaTableReader:
    logger: logging.Logger
    SUPPORTED_MODES = {"full", "backfill", "incremental"}

    def __init__(self, spark, config: dict, debug: bool = False):
        self.spark = spark
        self.config = config
        self.debug = debug
        self.catalog_sink = Environment.catalog_sink

    @DecoratorUtil.log_function()
    def read_stream(self) -> Tuple[DataFrame, str]:
        """
        Reads data from a Delta source using one of the following modes:
        - "full": batch read, with optional timestamp filter
        - "backfill": batch read for last N days
        - "incremental": streaming read using Change Data Feed (CDF),
                         with automatic fallback to batch backfill
        """
        from mdd.utils import DeltaTableUtil

        mode = self.config.get("mode")
        source_name = self.config.get("source_name")
        self.source_full_name = DeltaTableUtil.get_table_full_name(source_name)

        if not source_name or not mode:
            msg = (
                "'source_name' and valid 'mode' dictionary must be provided in config."
            )
            self.logger.error(msg)
            raise ValueError(msg)

        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forName(self.spark, self.source_full_name)
            version_range = (
                delta_table.history()
                .select("version")
                .rdd.map(lambda r: r[0])
                .collect()
            )
        except Exception as e:
            msg = f"Failed to retrieve Delta history for '{self.source_full_name}': {e}"
            self.logger.error(msg)
            raise RuntimeError(msg)

        if not version_range:
            msg = f"No version history found for source: {self.source_full_name}"
            self.logger.error(msg)
            raise RuntimeError(msg)

        self.min_version = min(version_range)
        self.max_version = max(version_range)

        # Read based on mode
        if mode == "full":
            value = self.config.get("full_max_processed_timestamp")
            df = self._read_full(self.source_full_name, value)
        elif mode == "backfill":
            value = self.config.get("backfill_days")
            if value is None:
                value = 7
                self.logger.warning(f"No backfill_days specified, defaulting for the last {value} days")
            df = self._read_backfill(self.source_full_name, value)
        elif mode == "incremental":
            value = self.config.get("incremental_max_processed_version")
            df, mode = self._read_incremental(self.source_full_name, value)
        else:
            msg = f"Unsupported mode '{mode}'. Supported modes: {self.SUPPORTED_MODES}"
            self.logger.error(msg)
            raise ValueError(msg)

        if df:
            # Add metadata
            from pyspark.sql.functions import col, lit
            df = (
                df.withColumn("_source_name", lit(source_name))
                .withColumn("_source_timestamp", col("_record_timestamp"))
            )

            if "_change_type" not in df.columns:
                df = (
                    df.withColumn("_change_type", lit("insert"))
                    .withColumn("_commit_version", lit(self.max_version))
                    .withColumn("_commit_timestamp", col("_record_timestamp"))
                )

            drop_columns = [
                "_record_id",
                "_record_timestamp",
            ]
            df = DeltaTableUtil.safe_drop_columns(df, drop_columns)

        return df, mode

    def _read_full(self, source_name: str) -> DataFrame:
        df = self.spark.readStream.format("delta").table(source_name)

        return df

    def _read_backfill(self, source_name: str, days: int | None) -> DataFrame:
        days = days or 7
        return (
            self.spark.readStream.format("delta")
            .table(source_name)
            .filter(
                to_date(col("_record_timestamp"))
                >= to_date(current_timestamp() - expr(f"INTERVAL {days} DAYS"))
            )
        )

    def _read_incremental(
        self, source_name: str, cdf_version: Optional[int]
    ) -> Tuple[Optional[DataFrame], str]:
        if cdf_version is None:
            self.logger.warning("No data processed, falling back to full mode")
            return self._read_full(source_name), "full"

        if cdf_version < self.min_version or cdf_version > self.max_version:
            fallback_days = self.config.get("fallback_backfill_days", 7)
            self.logger.warning(
                f"CDF version {cdf_version} out of range [{self.min_version}, {self.max_version}]. "
                f"Falling back to backfill ({fallback_days} days)."
            )
            return self._read_backfill(source_name, fallback_days), "backfill"

        if cdf_version == self.max_version:
            self.logger.info(f"No new data, max version read: {self.max_version}")
            return None, "incremental"

        if self.debug:
            self.logger.debug(
                f"Reading incremental data from version {cdf_version + 1} to {self.max_version}"
            )

        df = (
            self.spark.readStream.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", cdf_version + 1)
            .option("endingVersion", self.max_version)
            .table(source_name)
        )
        return df, "incremental"
