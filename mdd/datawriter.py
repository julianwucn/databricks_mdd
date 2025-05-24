import inspect
import logging
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType
from mdd.environment import Environment
from mdd.helper.decorator import DecoratorUtils
from mdd.helper.function import FunctionUtils as util
from mdd.helper.deltatable import DeltaTableUtils as dtu


@DecoratorUtils.add_logger()
class DeltaTableWriter:
    def __init__(
        self, spark: SparkSession, df: DataFrame, config: dict, debug: bool = False
    ):

        self.spark = spark
        self.df = df
        self.debug = debug

        self.sink_name = dtu.qualify_table_name(config['sink_name'])
        self.sink_projected_script = config["sink_projected_script"]
        self.sink_write_mode = config["sink_write_mode"]
        self.sink_primarykey = config["sink_primarykey"]
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

        try:
            location_key = "checkpointLocation"
            if location_key not in self.sink_write_options:
                raise ValueError(
                    "{location_key} must be specified in sink_write_options for streaming writes"
                )
            else:
                # rebuild the location
                location = f"{Environment.root_path_autoloader}/{config['sink_name']}/_checkpoint"
                checkpointLocation = self.sink_write_options[location_key]
                if checkpointLocation is not None:
                    # support mutliple sources
                    location = f"{location}/{checkpointLocation}"
                self.sink_write_options[location_key] = location

            if self.debug:
                self.logger.debug(
                    f"{location_key}: {self.sink_write_options[location_key]}"
                )

            if self.sink_write_mode == "update" and not self.sink_primarykey:
                raise ValueError("Primary key must be specified for update mode.")

            valid_modes = ["append", "update"]
            if self.sink_write_mode not in valid_modes:
                raise ValueError(
                    f"Invalid sink_write_mode '{self.sink_write_mode}'. Must be one of {valid_modes}"
                )

        except Exception as e:
            self.logger.error(f"Initialization error: {e}")
            raise

    def _get_trigger_args(self):
        if self.sink_write_trigger == "processingTime":
            return {"processingTime": self.sink_write_trigger_value}
        elif self.sink_write_trigger in ["once", "availableNow"]:
            return {self.sink_write_trigger: True}
        else:
            raise ValueError(
                "Unsupported trigger type '{self.sink_write_trigger}'. Must be one of ['processingTime', 'once', 'availableNow']"
            )

    @DecoratorUtils.log_function()
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

            temp_view = util.get_temp_view_name()
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

    @DecoratorUtils.log_function()
    def _match_columns(self, df: DataFrame, sink_name: str) -> DataFrame:
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
        from mdd.helper.deltatable import DeltaTableUtils as dtu

        source_columns = set(df.columns)
        target_df = self.spark.table(sink_name)

        # Get generated columns to exclude from comparison
        generated_columns = dtu.get_generated_columns(self.spark, sink_name)

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
            if self.debug:
                self.logger.debug(error_message)
            raise ValueError(error_message)

        # Attempt to cast source DataFrame columns to match target schema
        casted_columns = []
        for col_name in target_column_names:
            target_type: DataType = target_column_types[col_name]
            try:
                casted_columns.append(F.col(col_name).cast(target_type).alias(col_name))
            except Exception as e:
                error_message = f"Failed to cast column '{col_name}' to type '{target_type.simpleString()}': {e}"
                if self.debug:
                    self.logger.debug(error_message)
                raise ValueError(error_message)

        # Return reordered and casted DataFrame
        return df.select(casted_columns)

    @DecoratorUtils.log_function()
    def _validate_source_columns_in_sink(self, df: DataFrame, sink_name: str):
        """
        Ensure all source columns in the DataFrame exist in the sink table schema,
        excluding any generated columns.

        :param df: DataFrame to validate
        :param sink_name: Fully qualified Delta table name
        :raises ValueError: If any source column does not exist in sink
        """
        from mdd.helper.deltatable import DeltaTableUtils as dtu

        source_columns = set(df.columns)
        sink_df = self.spark.table(sink_name)
        generated_columns = dtu.get_generated_columns(self.spark, sink_name)
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
            if self.debug:
                self.logger.debug(error_message)
            raise ValueError(error_message)

    @DecoratorUtils.log_function()
    def _upsert_to_delta(self, micro_batch_df: DataFrame, batch_id: int):
        """
        Performs Delta upsert:
        - Inserts new records with all fields except generated columns (includes _record_id)
        - Updates existing records only if non-generated, meaningful fields have changed
        - Skips updating technical or generated columns
        """
        from mdd.helper.deltatable import DeltaTableUtils as dtu

        try:
            if self.debug:
                self.logger.debug(
                    f"Upserting batch {batch_id} to {self.sink_name} | sink_update_changes_only={self.sink_update_changes_only}"
                )

            alias_df = "updates"
            alias_table = "target"

            # === Get join condition on primary key ===
            join_condition = dtu.get_join_condition(
                alias_df, alias_table, self.sink_primarykey
            )

            # === Get generated columns from table ===
            generated_columns = dtu.get_generated_columns(self.spark, self.sink_name)

            all_columns = micro_batch_df.columns

            # === Fields to exclude from change detection ===
            exclude_from_comparison = set(
                generated_columns
                + [
                    "_record_id",
                    "_record_timestamp",
                    "_source_name",
                    "_source_timestamp",
                ]
            )

            # === Fields to exclude from update (same as above but keep _record_timestamp if needed) ===
            exclude_from_update = set(generated_columns + ["_record_id"])

            # Build change detection condition
            change_columns = [
                col for col in all_columns if col not in exclude_from_comparison
            ]
            change_conditions = [
                f"{alias_df}.{col} IS DISTINCT FROM {alias_table}.{col}"
                for col in change_columns
            ]
            change_filter = " OR ".join(change_conditions)

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

            delta_table = DeltaTable.forName(self.spark, self.sink_name)

            merge_builder = delta_table.alias(alias_table).merge(
                micro_batch_df.alias(alias_df), join_condition
            )

            if self.sink_update_changes_only:
                merge_builder = merge_builder.whenMatchedUpdate(
                    condition=change_filter, set=update_set
                )
            else:
                merge_builder = merge_builder.whenMatchedUpdate(set=update_set)

            merge_builder.whenNotMatchedInsert(values=insert_set).execute()

        except Exception as e:
            self.logger.error(f"Upsert failed for batch {batch_id}: {e}")
            raise

    @DecoratorUtils.log_function()
    def _filter_already_processed_files(self, df: DataFrame) -> tuple:
        """
        Filters out records from files already processed (based on _source_name and _source_timestamp)
        for 'append' and 'update' output modes only.

        :param df: Input DataFrame
        :return: Tuple(filtered DataFrame, list of already processed files as strings)
        """
        if self.sink_write_mode not in ["append", "update"]:
            if self.debug:
                self.logger.debug(
                    "Skipping file-based filtering in 'complete' output mode."
                )
            return df, []

        required_cols = {"_source_name", "_source_timestamp"}
        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            raise ValueError(
                f"Missing required metadata columns in stream: {missing_cols}"
            )

        existing_files_df = (
            self.spark.table(self.sink_name)
            .select("_source_name", "_source_timestamp")
            .distinct()
        )

        # Identify already processed files
        already_processed_df = (
            df.join(
                existing_files_df.hint("broadcast"),
                on=["_source_name", "_source_timestamp"],
                how="inner",
            )
            .select("_source_name", "_source_timestamp")
            .distinct()
        )

        # Collect already processed files info as strings for logging
        already_processed_files = [
            f"{row['_source_name']} (timestamp: {row['_source_timestamp']})"
            for row in already_processed_df.collect()
        ]

        # Filter out already processed files
        df_filtered = df.join(
            existing_files_df.hint("broadcast"),
            on=["_source_name", "_source_timestamp"],
            how="left_anti",
        )

        return df_filtered, already_processed_files

    @DecoratorUtils.log_function()
    def _filter_late_records(self, df: DataFrame) -> tuple:
        """
        Filters out records that are older or equal based on the watermark column
        when primary keys match against the target Delta table.

        Applies only in 'update' mode with specified watermark column.

        :param df: Input DataFrame.
        :return: Tuple(filtered DataFrame, count of filtered records).
        """
        if self.sink_write_mode != "update" or not self.sink_watermark_column:
            if self.debug:
                self.logger.debug(
                    "Skipping late record filtering because sink_write_mode is not 'update' or no specified watermark column."
                )
            return df, 0

        if not self.sink_primarykey or not isinstance(self.sink_primarykey, str):
            raise ValueError(
                "Primary key must be defined as a comma-separated string in self.sink_primarykey"
            )

        if self.sink_watermark_column not in df.columns:
            raise ValueError(
                f"Watermark column '{self.sink_watermark_column}' not found in input DataFrame"
            )

        try:
            key_list = util.string_to_list(self.sink_primarykey)
            target_df = (
                self.spark.table(self.sink_name)
                .selectExpr(*key_list, self.sink_watermark_column)
                .distinct()
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to read from sink table '{self.sink_name}': {e}"
            )
            return df, 0

        alias_df = "updates"
        alias_target = "target"

        join_condition = dtu.get_join_condition(
            alias_df, alias_target, self.sink_primarykey
        )

        target_df_broadcast = F.broadcast(target_df)

        joined_df = df.alias(alias_df).join(
            target_df_broadcast.alias(alias_target), on=join_condition, how="left"
        )

        filtered_df = joined_df.filter(
            (F.col(f"{alias_target}.{self.sink_watermark_column}").isNull())
            | (
                F.col(f"{alias_df}.{self.sink_watermark_column}")
                > F.col(f"{alias_target}.{self.sink_watermark_column}")
            )
        ).select(f"{alias_df}.*")

        filtered_count = df.count() - filtered_df.count()

        if self.debug:
            self.logger.debug(
                f"Filtered out {filtered_count} late or duplicate records based on primary key and watermark."
            )

        return filtered_df, filtered_count

    @DecoratorUtils.log_function()
    def write_stream(self):
        """
        Configure and start the streaming write based on the provided settings.
        """
        from pyspark.sql import functions as F

        def foreach_batch_function(batch_df, batch_id):
            # Run write prescript if defined
            if self.sink_write_prescript:
                self.spark.sql(self.sink_write_prescript)

            # Filter already processed files and get the list for logging
            filtered_df, already_processed_files = self._filter_already_processed_files(
                batch_df
            )

            if already_processed_files:
                self.logger.warning(
                    f"Batch {batch_id}: Already processed files skipped: {already_processed_files}"
                )

            # Filter late records and get the count for logging
            filtered_df, late_records_count = self._filter_late_records(filtered_df)

            if late_records_count > 0:
                self.logger.warning(
                    f"Batch {batch_id}: {late_records_count} late records filtered based on watermark."
                )

            if already_processed_files:
                self.logger.warning(
                    f"Batch {batch_id}: Already processed files skipped: {already_processed_files}"
                )

            if self.sink_write_mode == "update":
                self._validate_source_columns_in_sink(filtered_df, self.sink_name)
                self._upsert_to_delta(filtered_df, batch_id)
            else:  # append mode
                matched_df = self._match_columns(filtered_df, self.sink_name)
                matched_df.write.format("delta").mode("append").saveAsTable(
                    self.sink_name
                )

            # Run post-script if defined
            if self.sink_write_postscript:
                self.spark.sql(self.sink_write_postscript)

        try:
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

            # Configure and start the streaming write
            # must use append mode with foreachBatch
            query = (
                df.writeStream.format("delta")
                .outputMode("append")
                .options(**self.sink_write_options)
                .trigger(**self._get_trigger_args())
                .foreachBatch(foreach_batch_function)
                .start()
            )

            return query

        except Exception as e:
            self.logger.error(f"Error in write_stream: {e}")
            raise