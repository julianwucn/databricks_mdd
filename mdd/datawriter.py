import inspect
import logging
import shutil
from typing import Callable, Optional
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType
from mdd.environment import Environment
from mdd.utils import DecoratorUtil, FunctionUtil, DeltaTableUtil


@DecoratorUtil.add_logger()
class DeltaTableWriter:
    logger: logging.Logger
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        df: DataFrame,
        debug: bool = False,
        sync_mode: str = None,
        func_transform: Optional[Callable[[DataFrame], DataFrame]] = None
    ):
        self.spark = spark
        self.df = df
        self.debug = debug
        self.sync_mode = sync_mode
        self.func_transform = func_transform

        self.sink_name = config['sink_name']
        self.sink_full_name = DeltaTableUtil.get_table_full_name(self.sink_name )
        self.sink_projected_script = config["sink_projected_script"]
        self.sink_write_mode = config["sink_write_mode"]
        self.sink_primarykey = config["sink_primarykey"]
        self.sink_deduplication_expr = config["sink_deduplication_expr"]
        self.sink_watermark_column = config["sink_watermark_column"]
        self.sink_update_changes_only = config["sink_update_changes_only"]
        self.sink_write_options = config["sink_write_options"]
        self.sink_write_trigger = config["sink_write_trigger"]
        self.sink_write_trigger_value = config["sink_write_trigger_value"]
        self.sink_write_prescript = config["sink_write_prescript"]
        self.sink_write_postscript = config["sink_write_postscript"]
        self.sink_validators = config["sink_validators"]

        if self.debug:
            self.logger.debug(f"sink_name: {self.sink_name}")
            self.logger.debug(f"sink_projected_script: {self.sink_projected_script}")
            self.logger.debug(f"sink_write_mode: {self.sink_write_mode}")
            self.logger.debug(f"sink_watermark_column: {self.sink_watermark_column}")
            self.logger.debug(
                f"sink_update_changes_only: {self.sink_update_changes_only}"
            )
            self.logger.debug(f"sink_primarykey: {self.sink_primarykey}")
            self.logger.debug(f"sink_write_options: {self.sink_write_options}")
            self.logger.debug(f"sink_write_trigger: {self.sink_write_trigger}")
            self.logger.debug(
                f"sink_write_trigger_value: {self.sink_write_trigger_value}"
            )
            self.logger.debug(f"sink_write_prescript: {self.sink_write_prescript}")
            self.logger.debug(f"sink_write_postscript: {self.sink_write_postscript}")
            self.logger.debug(f"sink_validators: {self.sink_validators}")

    def _get_trigger_args(self):
        if self.sink_write_trigger == "processingTime":
            return {"processingTime": self.sink_write_trigger_value}
        elif self.sink_write_trigger in ["once", "availableNow"]:
            return {self.sink_write_trigger: True}
        else:
            raise ValueError(
                "Unsupported trigger type '{self.sink_write_trigger}'. Must be one of ['processingTime', 'once', 'availableNow']"
            )

    @DecoratorUtil.log_function()
    def _projected(self, df: DataFrame, projected_script: str) -> DataFrame:
        """
        Projects the DataFrame based on the specified SQL script.

        Enhancements:
        1. Raises an error if the view placeholder is not present in the projected script.
        2. Raises an error if any system columns (prefixed with '_') are missing in the returned DataFrame.

        :param df: Input DataFrame
        :param projected_script: SQL script with view placeholder 'vw_temp_source_data_uuid'
        :return: Projected DataFrame
        """
        try:
            if not projected_script or projected_script.strip() == "":
                return df

            placeholder = "vw_temp_source_data_uuid"
            if placeholder not in projected_script:
                message = f"View placeholder '{placeholder}' not found in projected_script '{projected_script}'."
                self.logger.error(message)
                raise ValueError(message)

            temp_view = FunctionUtil.get_temp_view_name()
            projected_sql = projected_script.replace(placeholder, temp_view)

            if self.debug:
                self.logger.debug(f"Projecting DataFrame using SQL: {projected_sql}")

            df.createOrReplaceTempView(temp_view)
            projected_df = self.spark.sql(projected_sql)

            # Check if all original system columns are present
            original_system_columns = {col for col in df.columns if col.startswith("_")}
            projected_columns = set(projected_df.columns)
            missing_system_columns = original_system_columns - projected_columns

            if missing_system_columns:
                message = (
                    f"Missing system columns after projection: {missing_system_columns}"
                )
                self.logger.error(message)
                raise ValueError(message)

            return projected_df

        except Exception as e:
            self.logger.error(f"Error in _projected: {e}")
            raise

    @DecoratorUtil.log_function()
    def _upsert_to_delta(self, micro_batch_df: DataFrame, batch_id: int):
        """
        Performs Delta upsert:
        - Inserts new records with all fields except generated columns (includes _record_id)
        - Updates existing records only if non-generated, meaningful fields have changed
        - Skips updating technical or generated columns
        """
        from mdd.utils import DeltaTableUtil

        try:
            if self.debug:
                self.logger.debug(
                    f"Upserting batch {batch_id} to {self.sink_name} | sink_update_changes_only={self.sink_update_changes_only}"
                )

            alias_df = "updates"
            alias_table = "target"

            # === Get join condition on primary key ===
            join_condition = DeltaTableUtil.get_join_condition(
                alias_df, alias_table, self.sink_primarykey
            )

            all_columns = micro_batch_df.columns
            primarykey_columns = FunctionUtil.string_to_list(self.sink_primarykey)
            generated_columns = DeltaTableUtil.get_generated_columns(self.spark, self.sink_name)

            # === Fields to exclude from change detection ===
            exclude_from_comparison = set(
                primarykey_columns
                + generated_columns
                + [
                    "_record_id",
                    "_record_timestamp",
                    "_source_name",
                    "_source_timestamp",
                ]
            )

            # === Fields to exclude from update (same as above but keep _record_timestamp if needed) ===
            exclude_from_update = set(primarykey_columns + generated_columns + ["_record_id"])

            # Build change detection condition
            change_filter = None
            if self.sink_update_changes_only:
                change_columns = [
                    col for col in all_columns if col not in exclude_from_comparison
                ]
                change_conditions = [
                    f"{alias_df}.{col} IS DISTINCT FROM {alias_table}.{col}"
                    for col in change_columns
                ]
                change_filter = " OR ".join(change_conditions)
            if self.sink_watermark_column:
                watermark_filter = f"{alias_df}.{self.sink_watermark_column} >= {alias_table}.{self.sink_watermark_column}"
                if change_filter:
                    change_filter = f"({change_filter}) AND ({watermark_filter})"
                else:
                    change_filter = watermark_filter

            # Update only allowed columns
            update_columns = [
                col for col in all_columns if col not in exclude_from_update
            ]
            update_set = {col: f"{alias_df}.{col}" for col in update_columns}

            # Insert all fields except generated columns
            insert_columns = [
                col for col in all_columns if col not in generated_columns
            ]
            insert_set = {col: f"{alias_df}.{col}" for col in insert_columns}

            if self.debug:
                self.logger.debug(f"Join condition: {join_condition}")
                self.logger.debug(f"Change detection condition: {change_filter}")
                self.logger.debug(f"Update set: {update_set}")
                self.logger.debug(f"Insert set: {insert_set}")

            # === Execute merge ===
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forName(self.spark, self.sink_full_name)

            merge_builder = (
                delta_table.alias(alias_table)
                .merge(micro_batch_df.alias(alias_df), join_condition)
                .whenMatchedUpdate(condition=change_filter, set=update_set)
                .whenNotMatchedInsert(values=insert_set)
            )

            merge_builder.execute()

        except Exception as e:
            self.logger.error(f"Upsert failed for batch {batch_id}: {e}")
            raise

    @DecoratorUtil.log_function()
    def _transform(self, df: DataFrame) -> DataFrame:
        """
        Apply the transformation logic to the input DataFrame.
        """
        return df

    @DecoratorUtil.log_function()
    def _process_microbatch(self, batch_df, batch_id):
        from mdd.utils import DeltaTableUtil
        self.logger.info(f"Processing batch {batch_id}")

        df = batch_df
        if self.func_transform:
            self.logger.info(f"Transforming data")
            df = self.func_transform(df)

            # check if the following system columns are included
            system_columns = ["_source_name", "_source_timestamp", "_record_deleted"]
            if not DeltaTableUtil.columns_exist(df, system_columns):
                raise ValueError(f"Missing system columns in the transformed data: {system_columns}")

        if self.sink_write_mode == "update":
            # verify columns existence
            DeltaTableUtil.validate_columns(self.spark, df, self.sink_name)

            # dedupicate
            df = DeltaTableUtil.deduplicate(df, self.sink_primarykey, self.sink_deduplication_expr)

            # update and insert
            self._upsert_to_delta(df, batch_id)

        else:
            # match the columns
            df = DeltaTableUtil.match_columns(self.spark, df, self.sink_name)

            # append
            df.write.format("delta").mode("append").saveAsTable(self.sink_full_name)

    @DecoratorUtil.log_function()
    def _execute_prescript(self):
        # Run pre-script if defined
        if self.sink_write_prescript:
            self.spark.sql(self.sink_write_prescript)

    @DecoratorUtil.log_function()
    def execute_postscript(self):
        # Run post-script if defined
        if self.sink_write_postscript:
            self.spark.sql(self.sink_write_postscript)


    @DecoratorUtil.log_function()
    def write_stream(self):
        """
        Configure and start the streaming write based on the provided settings.
        """
        from pyspark.sql import functions as F

        try:
            # run pre script
            self._execute_prescript()

            # Apply projection script
            df = self._projected(self.df, self.sink_projected_script)

            # Add system columns early
            if "_metadata" in df.columns:
                df = (
                    df.withColumn(
                        "_source_name", F.col("_metadata.file_name")
                    ).withColumn(
                        "_source_timestamp", F.col("_metadata.file_modification_time")
                    )
                ).drop(
                    "_metadata"
                )  # Drop _metadata after extracting needed info

            df = (
                df.withColumn("_record_id", F.expr("uuid()"))
                .withColumn("_record_timestamp", F.current_timestamp())
                .withColumn("_record_deleted", F.lit(False))
            )

            # rebuild teh checkpoint location based on sync_mode
            location_key = "checkpointLocation"
            checkpointlocation = self.sink_write_options.get(location_key)
            checkpointlocation_path = DeltaTableUtil.get_checkpointlocation_path(
                self.sink_name,
                checkpointlocation,
                self.sync_mode)
            if self.debug:
                    self.logger.debug(f"Checkpoint location: {checkpointlocation_path} for sync mode '{self.sync_mode}'")

            

            self.sink_write_options[location_key] = checkpointlocation_path

            # Configure and start the streaming write
            # must use append mode with foreachBatch
            query = (
                df.writeStream.format("delta")
                .outputMode("append")
                .options(**self.sink_write_options)
                .trigger(**self._get_trigger_args())
                .foreachBatch(self._process_microbatch)
                .start()
            )

            return query

        except Exception as e:
            self.logger.error(f"Error in write_stream: {e}")
            raise


