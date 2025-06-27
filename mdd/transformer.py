import logging
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, expr, current_timestamp, lit
from mdd.utils import DecoratorUtil, DeltaTableUtil, FunctionUtil, MDDUtil
from mdd.metadata import Metadata
from mdd.datareader import DeltaTableReader
from mdd.datawriter import DeltaTableWriter


@DecoratorUtil.add_logger()
class TransformerABS:
    logger: logging.Logger
    debug: bool
    development: bool

    def __init__(self, spark: SparkSession, metadata_yml: str, development: bool = False):
        self.development = development
        self.spark = spark
        self.metadata = Metadata(metadata_yml, False)
        self.debug = self.metadata.get("debug")
        self.active = self.metadata.get("active")
        self.source_name = self.metadata.get("reader", "source_name")
        self.source_primarykey = self.metadata.get("reader", "source_primarykey")
        self.sink_name = self.metadata.get("writer", "sink_name")
        self.sink_write_mode = self.metadata.get("writer", "sink_write_mode")
        self.mode = self.metadata.get("sync_options", "mode")
        self.backfill_days = self.metadata.get("sync_options", "backfill_days")

        dataflow_type = self.metadata.get("dataflow_type")
        if dataflow_type != "transform":
            message = (
                f"Invalid dataflow type: {dataflow_type}, it should be 'transform'"
            )
            self.logger.error(message)
            raise Exception(message)

        DeltaTableUtil.ensure_system_columns(self.spark, self.sink_name)

    @DecoratorUtil.log_function()
    def _read_stream(self) -> Tuple[DataFrame, str, int]:
        """
        Reads data from the specified Delta table using the provided configuration.

        :return: Spark DataFrame containing the read data
        """
        # build the config for data reader
        config = {}
        config["source_name"] = self.source_name
        config["mode"] = self.mode
        config["backfill_days"] = self.backfill_days
        if self.mode == "full":
            # get the max _source_timestamp from the target table
            max_source_timestamp = DeltaTableUtil.get_max_column_value(
                self.spark, self.sink_name, "_source_timestamp"
            )
            config["full_max_processed_timestamp"] = max_source_timestamp
        elif self.mode == "incremental":
            max_processed_version = MDDUtil.get_processed_version(
            self.spark,
            self.sink_name,
            self.source_name,
        )
            config["incremental_max_processed_version"] = max_processed_version
            self.logger.info(f"Last processed version: {max_processed_version}")
        else:
            message = f"Invalid mode: {self.mode}"
            self.logger.error(message)
            raise ValueError(message)

        if self.debug:
            self.logger.debug(f"Config: {config}")

        from mdd.datareader import DeltaTableReader
        reader = DeltaTableReader(spark=self.spark, config=config, debug=self.debug)
        df, sync_mode = reader.read_stream()

        return df, sync_mode, reader.max_version

    @DecoratorUtil.log_function()
    def _transform(self, df: DataFrame) -> DataFrame | None:
        """
        Applies transformations to the input DataFrame.

        :param df: Input DataFrame
        :return: Transformed DataFrame
        """
        df = self._get_clean_data(df)

        return df

    @DecoratorUtil.log_function()
    def _get_clean_data(self, df: DataFrame):
        from mdd.utils import DeltaTableUtil
        source_deduplication_expr = "_commit_version desc"
        df = DeltaTableUtil.deduplicate(df, self.source_primarykey, source_deduplication_expr)

        condition = expr("""
            CASE
                WHEN _record_deleted = true
                    OR (_corrupt_record IS NOT NULL AND _corrupt_record != '')
                    OR _change_type = 'delete'
                THEN true
                ELSE false
            END
        """)

        df = (
            df.withColumn("_record_deleted", condition)
            .withColumn("_source_name", lit(self.source_name))
            .withColumn("_source_timestamp", col("_record_timestamp"))
        )

        drop_columns = [
            "_corrupt_record",
            "_rescued_data",
            "_record_id",
            "_record_timestamp",
            "_change_type",
            "_commit_version",
            "_commit_timestamp"
        ]
        df = DeltaTableUtil.safe_drop_columns(df, drop_columns)

        return df

    @DecoratorUtil.log_function()
    def _write_stream(self, df: DataFrame, sync_mode: str, max_version: int):
        from mdd.datawriter import DeltaTableWriter
        DeltaTableUtil.ensure_system_columns(self.spark, self.sink_name)

        config_writer = self.metadata.get("writer")
        writer = DeltaTableWriter(
            self.spark, config_writer, df, self.debug, sync_mode, self._transform
        )
        query = writer.write_stream()
        query.awaitTermination()

        # log the processed cdf version
        if self.development:
            self.logger.info(f"Processed version not saved for development")
        else:
            self.logger.info(f"Processed version: {max_version} | mode: {sync_mode}")
            MDDUtil.save_processed_version(
                self.spark,
                self.sink_name,
                self.source_name,
                max_version,
                sync_mode
            )

        # run post script
        writer.execute_postscript()

    @DecoratorUtil.log_function()
    def run(self):
        transform_name = f"{self.source_name} => {self.sink_name}"

        if not self.active:
            self.logger.info(f"Transform skipped (inactive): {transform_name}")
            return

        self.logger.info(f"Transform start: {transform_name}")

        self.logger.info(f"Read data: {self.source_name} | mode: {self.mode}")
        df, sync_mode, max_version = self._read_stream()

        self.logger.info(f"Write data: {self.sink_name} | mode: {self.sink_write_mode}")
        if df:
            self._write_stream(df, sync_mode, max_version)
        else:
            self.logger.info(f"No data, write skipped")

        self.logger.info(f"Transform end: {transform_name}")