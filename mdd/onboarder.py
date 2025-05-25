from mdd.utils import DecoratorUtil, DeltaTableUtil
from mdd.metadata import Metadata
from mdd.datareader import AutoLoaderReader
from mdd.datawriter import DeltaTableWriter
from pyspark.sql import SparkSession
import logging

@DecoratorUtil.add_logger()
class OnboardDataFlow:
    logger: logging.Logger
    def __init__(self, spark: SparkSession, metadata_yml: str):
        self.spark = spark
        self.metadata = Metadata(metadata_yml, False)
        self.debug = self.metadata.get("debug")

        dataflow_type = self.metadata.get("dataflow_type")
        if dataflow_type != "onboard":
            message = f"Invalid dataflow type: {dataflow_type}, it should be 'onboard'"
            self.logger.error(message)
            raise Exception(message)

    @DecoratorUtil.log_function()
    def run(self):
        active = self.metadata.get("active")
        sink_name = self.metadata.get("writer", "sink_name")
        source_relative_path = self.metadata.get("reader", "source_relative_path")

        onboard_name = f"{source_relative_path} => {sink_name}"
        self.logger.info(f"Onboard data start: {onboard_name}")

        if not active:
            message = f"Dataflow is not active: {self.metadata_yml_path}"
            self.logger.warning(message)
            return None
        
        # validate table existence and ensure system columns existence
        self.logger.info(f"Sink validation: {sink_name}")
        corrupt_record = self.metadata.get("reader", "_corrupt_record") 
        rescued_data = self.metadata.get("reader", "_rescued_data") 
        DeltaTableUtil.ensure_system_columns(self.spark, sink_name, corrupt_record, rescued_data, False)

        self.logger.info(f"Read data: {source_relative_path}")

        # get reader config
        config_reader = self.metadata.get("reader")
        # pass in the sink_name
        config_reader["sink_name"] = sink_name

        # read the data
        reader = AutoLoaderReader(self.spark, config_reader, self.debug)
        df = reader.read_stream()

        self.logger.info(f"Write data: {sink_name}")

        # get writer config
        config_writer = self.metadata.get("writer")
        
        # write the data
        writer = DeltaTableWriter(self.spark, df, config_writer, self.debug)
        query = writer.write_stream()
        query.awaitTermination()

        self.logger.info(f"Onboard data end: {onboard_name}")

