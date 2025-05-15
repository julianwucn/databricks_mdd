#!/usr/bin/env python
# coding: utf-8

# ## datawriter
# 
# New notebook

# In[ ]:


from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from delta.tables import *
import uuid
import json
import datetime as mdt
import logging

from env.mdd.utilhelper import *
from env.mdd.validator import *


# In[ ]:


class DataWriter:

    def __init__(self, format, schema_bronze, schema_silver, schema_gold, spark, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"format: {format}")

        self.debug = debug
        self.format = format
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold
        self.spark = spark
        self.logger = logger

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[ ]:


# write to onboarded table
class OnboardDataWriter(DataWriter):

    def __init__(self, source_data_df, destination_table_full_path, destination_table_name, destination_write_mode, \
                destination_write_options, destination_projected_sql, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, debug = False):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__("delta", schema_bronze, schema_silver, schema_gold, spark, logger, debug)
        self.source_data_df = source_data_df
        self.destination_table_full_path = destination_table_full_path
        self.destination_table_name = destination_table_name
        self.destination_write_mode = destination_write_mode
        self.destination_write_options = destination_write_options 
        self.destination_projected_sql = destination_projected_sql
        self.metadata_validator_yml = metadata_validator_yml

        if debug:
            logger.debug(f"source_data_df: {source_data_df}")
            logger.debug(f"destination_table_full_path: {destination_table_full_path}")
            logger.debug(f"destination_table_name: {destination_table_name}")
            logger.debug(f"destination_write_mode: {destination_write_mode}")
            logger.debug(f"destination_write_options: {destination_write_options}")
            logger.debug(f"destination_projected_sql: {destination_projected_sql}")
            logger.debug(f"metadata_validator_yml: {metadata_validator_yml}")

            logger.debug(f"function end: {function_name}")
    # function end: init

    # project the source data dataframe
    def project(self, source_data_df, destination_projected_sql):
        function_name = "project"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_data_df: {source_data_df.count()}")
            display(source_data_df)
        
        if destination_projected_sql is None or destination_projected_sql == "":
            destination_projected_sql = "select * from <vw_temp_source_data_uuid>"
        if self.debug:
            self.logger.debug(f"destination_projected_sql: {destination_projected_sql}")
        
        # generate a unique temp view name
        vw_temp_source_data_uuid = f"vw_temp_source_data_{uuid.uuid4().hex}"
        destination_projected_sql_uuid = destination_projected_sql.replace("<vw_temp_source_data_uuid>", vw_temp_source_data_uuid).replace("vw_temp_source_data_uuid", vw_temp_source_data_uuid)
        if self.debug:
            self.logger.debug(f"vw_temp_source_data_uuid: {vw_temp_source_data_uuid}")
            self.logger.debug(f"destination_projected_sql_uuid: {destination_projected_sql_uuid}")

        source_data_df.createOrReplaceTempView(vw_temp_source_data_uuid)
        source_data_projected_df = self.spark.sql(destination_projected_sql_uuid) \
                                .withColumn("_deleted_by_source", lit(False)) \
                                .withColumn("_deleted_by_validation", lit(False)) \
                                .withColumn("_data_validation_expectations", lit("")) \
                                .withColumn("_source_name", col("_metadata.file_name")) \
                                .withColumn("_source_timestamp", col("_metadata.file_modification_time").cast("timestamp")) \
                                .withColumn("_record_timestamp",lit(current_timestamp())) \
                                .drop("_metadata")
        


        if self.debug:
            self.logger.debug(f"source_data_projected_df: {source_data_projected_df.count()}")
            source_data_projected_df.printSchema()
            self.logger.debug(f"function end: {function_name}")

        return source_data_projected_df
    # function end: project 


    def save(self, source_data_df, destination_table_full_path, destination_table_name, \
                    destination_write_mode, destination_write_options):
        function_name = "save"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_data_df: {source_data_df.count()}")
            source_data_df.printSchema()
            display(source_data_df)
            self.logger.debug(f"destination_table_full_path: {destination_table_full_path}")
            self.logger.debug(f"destination_table_name: {destination_table_name}")
            self.logger.debug(f"destination_write_mode: {destination_write_mode}")
            self.logger.debug(f"destination_write_options: {destination_write_options}")
        
        if "mergeSchema" in destination_write_options.keys():
            if not (destination_write_options["mergeSchema"] is None):
                if self.debug:
                    self.logger.debug(f"set spark.databricks.delta.schema.autoMerge.enabled = {destination_write_options['mergeSchema']}")
                self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", destination_write_options["mergeSchema"])

        # generate new uuid for new data written to destination
        column_uuid = "_record_uuid"
        # drop the uuid of the source and then regeneate new uuid for destination
        if column_uuid in source_data_df.schema.names:
            source_data_df = source_data_df.drop(column_uuid)
        # use the udf to generate the uuid for each record
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        source_data_df = source_data_df.withColumn(column_uuid, uuid_udf())
        if self.debug:
            self.logger.debug(f"source_data_df: {source_data_df.count()}")
            source_data_df.printSchema()
            display(source_data_df)

        if destination_write_mode == "merge":
            source_alias = "source"
            target_alias = "target"

            # get the to-be-updated columns excluding the columns in primary key
            tableutil = TableUtil(destination_table_name, self.spark, self.debug)
            table_property_dict = tableutil.get_table_properties()
            destination_table_primarykey = table_property_dict["mdd.primaryKey"]
            if destination_table_primarykey is None or len(destination_table_primarykey) == 0:
                error_msg = f"table property '{table_property_pk}' has no value on table '{destination_table_name}'"
                self.logger.error(error_msg)
                raise Exception(error_msg)

            columns_join = destination_table_primarykey
            columns_all = source_data_df.schema.names
            columns_all.remove(column_uuid) # remove the column_uuid because it could not be inlcuded in the update
            columns_to_update = [column for column in columns_all if column not in columns_join]
            columns_to_update_condition = [column for column in columns_all if column not in columns_join and column not in ["_record_timestamp", "_source_name", "_source_timestamp"]]
            merge_join_condition = " AND ".join([f"{target_alias}.{col} = {source_alias}.{col}" for col in columns_join])
            merge_update_condition = " OR ".join([f"({target_alias}.{col} <> {source_alias}.{col} OR {target_alias}.{col} IS NULL AND {source_alias}.{col} IS NOT NULL OR {target_alias}.{col} IS NOT NULL AND {source_alias}.{col} IS NULL)" for col in columns_to_update_condition])
            merge_update_set = dict([(f"{target_alias}.{key}", f"{source_alias}.{val}") for key, val in zip(columns_to_update[::1], columns_to_update[::1])]) 

            # get watermark column
            tableutil = TableUtil(destination_table_name, self.spark, self.debug)
            table_property_dict = tableutil.get_table_properties()
            table_property_watermark_column = "mdd.watermarkColumn"
            table_watermark_column = None
            if table_property_watermark_column in table_property_dict:
                table_watermark_column = table_property_dict[table_property_watermark_column]
            
            # update the record only when the source record is not older than the existing destination record if the table has a watermark column
            if not destination_write_options["merge_update_changed_rows_only"]:
                merge_update_condition = "True"

            if table_watermark_column is not None and table_watermark_column != "":
                merge_update_condition = f"{merge_update_condition} AND ({source_alias}.{table_watermark_column} >= {target_alias}.{table_watermark_column})"
            
            if self.debug:
                self.logger.debug(f"columns_join: {columns_join}")
                self.logger.debug(f"columns_all: {columns_all}")
                self.logger.debug(f"columns_to_update: {columns_to_update}")
                self.logger.debug(f"merge_join_condition: {merge_join_condition}")
                self.logger.debug(f"table_watermark_column: {table_watermark_column}")
                self.logger.debug(f"merge_update_condition: {merge_update_condition}")
                self.logger.debug(f"merge_update_set: {merge_update_set}")

            # get the destination table
            #destination_data_dt = DeltaTable.forName(spark, destination_table_name) #not working in Fabric
            destination_data_dt = DeltaTable.forPath(self.spark, destination_table_full_path)
            if self.debug:
                destination_data_df = destination_data_dt.toDF()
                self.logger.debug(f"destination_data_dt: {destination_data_df.count()}")
                destination_data_df.printSchema()
                display(destination_data_df)

            file_list = tuple(set(source_data_df.select("_source_name").toPandas()["_source_name"])) + ("", )
            #print(f"file_list: {file_list}")
            # merge the data
            merge_timestamp_from = mdt.datetime.now()
            (
                destination_data_dt.alias(target_alias)
                                    .merge(source = source_data_df.alias(source_alias), condition = merge_join_condition)
                                    .whenNotMatchedInsertAll()
                                    .whenMatchedUpdate(condition = merge_update_condition, set = merge_update_set)
                                    .whenNotMatchedBySourceUpdate(condition = f"{target_alias}._source_name in {file_list}"
                                        ,set = {f"{target_alias}._deleted_by_source": "True"
                                            ,f"{target_alias}._record_timestamp": lit(current_timestamp())
                                            })
                                    .execute()
            )
            merge_timestamp_to = mdt.datetime.now()
            merge_info_dict = tableutil.get_table_merge_info(merge_timestamp_from, merge_timestamp_to)
            if self.debug:
                self.logger.debug(f"merge_timestamp_from: {merge_timestamp_from}")
                self.logger.debug(f"merge_timestamp_to: {merge_timestamp_to}")
                self.logger.debug(f"merge_info_dict: {merge_info_dict}")

            if merge_info_dict is not None and len(merge_info_dict) > 0:
                numSourceRows = merge_info_dict["numSourceRows"]
                numTargetRowsInserted = merge_info_dict["numTargetRowsInserted"]
                numTargetRowsUpdated = merge_info_dict["numTargetRowsUpdated"]
                numTargetRowsDeleted = merge_info_dict["numTargetRowsDeleted"]
                numSourceRows = int(numTargetRowsInserted) + int(numTargetRowsUpdated) + int(numTargetRowsDeleted)
                
                #self.logger.info(f"total source rows: {numSourceRows}")
                self.logger.info(f"rows inserted: {numTargetRowsInserted}")
                self.logger.info(f"rows updated: {numTargetRowsUpdated}")
                self.logger.info(f"rows deleted: {numTargetRowsDeleted}")
            

        else:
            # overwrite or append the data
            source_data_df.write.mode(destination_write_mode).options(**destination_write_options).saveAsTable(destination_table_name)
            self.logger.info(f"total rows inserted: {source_data_df.count()}")


        if self.debug:
            self.logger.debug(f"function end: {function_name}")
    # function end: save

    def write(self):
        function_name = "write"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        # project the source data
        source_data_projected_df = self.project(self.source_data_df, self.destination_projected_sql)
        
        # validate the rows
        self.logger.info(f"row validation start")
        if self.metadata_validator_yml is not None and self.metadata_validator_yml != "":
            validator_row = RowValidator(self.metadata_validator_yml, self.schema_bronze, self.schema_silver, self.schema_gold, self.spark, self.debug)
            source_data_validated_df = validator_row.validate(self.destination_table_name, source_data_projected_df)
        else:
            source_data_validated_df = source_data_projected_df
        self.logger.info(f"row validation end")

        # write the validated data into destination table
        self.save(source_data_validated_df, self.destination_table_full_path, self.destination_table_name, \
                    self.destination_write_mode, self.destination_write_options)

        if self.debug:
            self.logger.debug(f"function end: {function_name}")


# In[ ]:


# write to tranformed table
class TransformDataWriter(DataWriter):

    def __init__(self, source_data_df, destination_table_full_path, destination_table_name, destination_write_mode, \
                destination_write_options, destination_projected_sql, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, debug = False):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__("delta", schema_bronze, schema_silver, schema_gold, spark, logger, debug)
        self.source_data_df = source_data_df
        self.destination_table_full_path = destination_table_full_path
        self.destination_table_name = destination_table_name
        self.destination_write_mode = destination_write_mode
        self.destination_write_options = destination_write_options 
        self.destination_projected_sql = destination_projected_sql
        self.metadata_validator_yml = metadata_validator_yml

        self.rows_inserted = 0
        self.rows_updated = 0
        self.rows_deleted = 0

        if debug:
            logger.debug(f"source_data_df: {source_data_df}")
            logger.debug(f"destination_table_full_path: {destination_table_full_path}")
            logger.debug(f"destination_table_name: {destination_table_name}")
            logger.debug(f"destination_write_mode: {destination_write_mode}")
            logger.debug(f"destination_write_options: {destination_write_options}")
            logger.debug(f"destination_projected_sql: {destination_projected_sql}")
            logger.debug(f"metadata_validator_yml: {metadata_validator_yml}")

            logger.debug(f"function end: {function_name}")
    # function end: init

    # project the source data dataframe
    def project(self, source_data_df, destination_projected_sql):
        function_name = "project"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_data_df: {source_data_df.count()}")
            display(source_data_df)
        
        source_data_projected_df = source_data_df

        if self.debug:
            self.logger.debug(f"source_data_projected_df: {source_data_projected_df.count()}")
            source_data_projected_df.printSchema()
            self.logger.debug(f"function end: {function_name}")

        return source_data_projected_df
    # function end: project 

    def save(self, source_data_df, destination_table_full_path, destination_table_name, \
                    destination_write_mode, destination_write_options):
        function_name = "save"
        self.logger.info(f"write start")

        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_data_df: {source_data_df.count()}")
            source_data_df.printSchema()
            display(source_data_df)
            self.logger.debug(f"destination_table_full_path: {destination_table_full_path}")
            self.logger.debug(f"destination_table_name: {destination_table_name}")
            self.logger.debug(f"destination_write_mode: {destination_write_mode}")
            self.logger.debug(f"destination_write_options: {destination_write_options}")
        
        if "mergeSchema" in destination_write_options.keys():
            if not (destination_write_options["mergeSchema"] is None):
                if self.debug:
                    self.logger.debug(f"set spark.databricks.delta.schema.autoMerge.enabled = {destination_write_options['mergeSchema']}")
                self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", destination_write_options["mergeSchema"])

        # reshape the dataframe
        # use the udf to generate the uuid for each record
        column_uuid = "_record_uuid"
        # drop the uuid of the source and then regeneate new uuid for destination
        if column_uuid in source_data_df.schema.names:
            source_data_df = source_data_df.drop(column_uuid)
        # use the udf to generate the uuid for each record    
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        columns_to_drop = ("_deleted_by_validation", "_record_timestamp", "_commit_version", "_commit_timestamp", "_change_type", "_corrupt_data")
        source_data_df = (
            source_data_df.withColumn("_deleted_by_source", source_data_df._deleted_by_source | source_data_df._deleted_by_validation | (source_data_df._change_type == "delete"))
                            .withColumn("_source_timestamp", source_data_df._record_timestamp) 
                            .withColumn("_source_cdfversion", source_data_df._commit_version)  
                            .withColumn(column_uuid, uuid_udf()) 
                            .drop(*columns_to_drop) 
                            .withColumn("_record_timestamp",lit(current_timestamp())) 
                            .withColumn("_deleted_by_validation", lit(False)) 
        )

        # in case no validator
        if "_data_validation_expectations" not in source_data_df.columns:
            source_data_df = source_data_df.withColumn("_data_validation_expectations", lit(""))

        if self.debug:
            self.logger.debug(f"source_data_df reshaped schema:")
            source_data_df.printSchema()

        if destination_write_mode == "merge":
            source_alias = "source"
            target_alias = "target"

            # get the to-be-updated columns excluding the columns in primary key
            tableutil = TableUtil(destination_table_name, self.spark, self.debug)
            table_property_dict = tableutil.get_table_properties()
            destination_table_primarykey = table_property_dict["mdd.primaryKey"]
            if destination_table_primarykey is None or len(destination_table_primarykey) == 0:
                error_msg = f"table property '{table_property_pk}' has no value on table '{destination_table_name}'"
                self.logger.error(error_msg)
                raise Exception(error_msg)

            # process scd2 dimension table ##########################################################
            # get scd2 config
            scd2_enabled = False
            if "scd2_enabled" in destination_write_options and destination_write_options["scd2_enabled"] is not None:
                    scd2_enabled = destination_write_options["scd2_enabled"]
            
            # rebuild the source_data_df before merge for scd2
            if scd2_enabled:
                scd2_columns = ""
                if "scd2_columns" in destination_write_options and destination_write_options["scd2_enabled"] is not None:
                        scd2_columns = destination_write_options["scd2_columns"]

                # must have scd2 columns if scd2 enabled
                if scd2_columns == "":
                    msg = f"'scd2_columns' cannot be empty"
                    self.logger.error(msg)
                    raise Exception(msg)

                # PK must have scd2_startdate
                if "scd2_startdate" not in destination_table_primarykey:
                    msg = f"'scd2_startdate' must be included in the primary key for scd2 tables"
                    self.logger.error(msg)
                    raise Exception(msg)

                # get current versions
                scd2_current_sql = f"select * from {destination_table_name} where scd2_iscurrent"
                scd2_current_df = self.spark.sql(scd2_current_sql)
                if self.debug:
                    self.logger.debug(f"scd2_current_sql: {scd2_current_sql}")
                    self.logger.debug(f"scd2_current_df: {scd2_current_df.count()}")

                # build the new data for merge
                # only join on original primary key and scd2 columns
                scd2_columns_list = [column.strip() for column in scd2_columns.split(",")]
                scd2_pk_columns_list = destination_table_primarykey.copy() # have to get a deep copy to make a brand new list, otherwise, it will still point to the old list
                scd2_pk_columns_list.remove("scd2_startdate")
                scd2_columns_join = list(set(scd2_pk_columns_list + scd2_columns_list))

                # 1. current versions for update
                # same pk and scd2 columns
                source_data_df_current1 = source_data_df.alias("df1").join(scd2_current_df.alias("df2"), scd2_columns_join, "inner") \
                                                                .select("df1.*", scd2_current_df.scd2_startdate, scd2_current_df.scd2_enddate, scd2_current_df.scd2_iscurrent)
                # same pk, same scd2 start date but different scd2 columns
                source_data_df_current2 = source_data_df.alias("df1").join(scd2_current_df.alias("df2"), scd2_pk_columns_list, "inner").filter("df2.scd2_startdate = current_date()") \
                                                                .select("df1.*", scd2_current_df.scd2_startdate, scd2_current_df.scd2_enddate, scd2_current_df.scd2_iscurrent)
                source_data_df_current2 = source_data_df_current2.alias("df1").join(source_data_df_current1.alias("df2"), scd2_pk_columns_list, "left").filter("df2._record_uuid is null").select("df1.*")

                # 2. new versions for insert
                # same pk, different scd2 columns, and not in source_data_df_current2
                source_data_df_new = source_data_df.alias("df1").join(scd2_current_df.alias("df2"), scd2_columns_join, "left").filter("df2._record_uuid is null") \
                                                                .select("df1.*").withColumn("scd2_startdate", lit(mdt.date.today())).withColumn("scd2_enddate", lit(None)).withColumn("scd2_iscurrent", lit(True))
                #source_data_df.show(truncate = False)
                #source_data_df_current2.show(truncate = False)
                #source_data_df_new.show(truncate = False)
                source_data_df_new = source_data_df_new.alias("df1").join(source_data_df_current2.alias("df2"), scd2_pk_columns_list, "left").filter("df2._record_uuid is null").select("df1.*")

                #3. old versions for update
                source_data_df_old = scd2_current_df.alias("df1").join(source_data_df_new.alias("df2"), scd2_pk_columns_list, "inner").select("df1.*") \
                                                                .withColumn("scd2_enddate", lit(mdt.date.today())).withColumn("scd2_iscurrent", lit(False)) \
                                                                .withColumn("_record_timestamp", lit(current_timestamp()))

                # bulid the final df
                source_data_df = source_data_df_current1.unionByName(source_data_df_current2).unionByName(source_data_df_new).unionByName(source_data_df_old)
                #source_data_df.show(truncate = False)

                if self.debug:
                    self.logger.debug(f"scd2_columns_list: {scd2_columns_list}")
                    self.logger.debug(f"scd2_columns_join: {scd2_columns_join}")
                    self.logger.debug(f"source_data_df_current1: {source_data_df_current1.count()}")
                    self.logger.debug(f"source_data_df_current2: {source_data_df_current2.count()}")
                    self.logger.debug(f"source_data_df_new: {source_data_df_new.count()}")
                    self.logger.debug(f"source_data_df_old: {source_data_df_old.count()}")
                    self.logger.debug(f"source_data_df: {source_data_df.count()}")
            # end if scd2_enabled #########################################################            

            columns_join = destination_table_primarykey
            columns_all = source_data_df.schema.names
            columns_to_update = [column for column in columns_all if column not in columns_join and column not in ["_change_type", column_uuid]]
            columns_to_update_condition = [column for column in columns_all if column not in columns_join and column not in ["_record_timestamp", "_change_type", column_uuid]]
            merge_join_condition = " AND ".join([f"{target_alias}.{col} = {source_alias}.{col}" for col in columns_join])
            merge_update_condition = " OR ".join([f"({target_alias}.{col} <> {source_alias}.{col} OR {target_alias}.{col} IS NULL AND {source_alias}.{col} IS NOT NULL OR {target_alias}.{col} IS NOT NULL AND {source_alias}.{col} IS NULL)" for col in columns_to_update_condition])
            merge_update_set = dict([(f"{target_alias}.{key}", f"{source_alias}.{val}") for key, val in zip(columns_to_update[::1], columns_to_update[::1])]) 
            
            #if not destination_write_options["merge_update_changed_rows_only"]:
            #    merge_update_condition = "True"
            
            if self.debug:
                self.logger.debug(f"columns_join: {columns_join}")
                self.logger.debug(f"columns_all: {columns_all}")
                self.logger.debug(f"columns_to_update: {columns_to_update}")
                self.logger.debug(f"merge_join_condition: {merge_join_condition}")
                self.logger.debug(f"merge_update_condition: {merge_update_condition}")
                self.logger.debug(f"merge_update_set: {merge_update_set}")

            # get the destination table
            #destination_data_dt = DeltaTable.forName(spark, destination_table_name) #not working in Fabric
            destination_data_dt = DeltaTable.forPath(self.spark, destination_table_full_path)
            if self.debug:
                self.logger.debug(f"source_data_df: {source_data_df.count()}")
                source_data_df.printSchema()
                destination_data_df = destination_data_dt.toDF()
                self.logger.debug(f"destination_data_dt: {destination_data_df.count()}")
                destination_data_df.printSchema()

            

            # merge the data
            merge_timestamp_from = mdt.datetime.now()
            (
                destination_data_dt.alias(target_alias)
                                    .merge(source = source_data_df.alias(source_alias), condition = merge_join_condition)
                                    .whenNotMatchedInsertAll()
                                    .whenMatchedUpdate(set = merge_update_set)
                                    .execute()
            )
            merge_timestamp_to = mdt.datetime.now()
            merge_info_dict = tableutil.get_table_merge_info(merge_timestamp_from, merge_timestamp_to)
            if self.debug:
                self.logger.debug(f"merge_timestamp_from: {merge_timestamp_from}")
                self.logger.debug(f"merge_timestamp_to: {merge_timestamp_to}")
                self.logger.debug(f"merge_info_dict: {merge_info_dict}")

            if merge_info_dict is not None and len(merge_info_dict) > 0:
                numSourceRows = merge_info_dict["numSourceRows"]
                numTargetRowsInserted = merge_info_dict["numTargetRowsInserted"]
                numTargetRowsUpdated = merge_info_dict["numTargetRowsUpdated"]
                numTargetRowsDeleted = merge_info_dict["numTargetRowsDeleted"]
                numSourceRows = int(numTargetRowsInserted) + int(numTargetRowsUpdated) + int(numTargetRowsDeleted)

                #self.logger.info(f"total source rows: {numSourceRows}")
                self.logger.info(f"  rows inserted: {numTargetRowsInserted}")
                self.logger.info(f"  rows updated: {numTargetRowsUpdated}")
                self.logger.info(f"  rows deleted: {numTargetRowsDeleted}")

                self.rows_inserted = int(numTargetRowsInserted)
                self.rows_updated = int(numTargetRowsUpdated)
                self.rows_deleted = int(numTargetRowsDeleted)

        else:
            # overwrite or append the data
            source_data_df.write.mode(destination_write_mode).options(**destination_write_options).saveAsTable(destination_table_name)
            self.rows_inserted = source_data_df.count()

        self.logger.info(f"write end")

        if self.debug:
            self.logger.debug(f"function end: {function_name}")
    # function end: save


    def write(self):
        function_name = "write"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        # project the source data
        source_data_projected_df = self.project(self.source_data_df, self.destination_projected_sql)
        
        # validate the rows
        if self.metadata_validator_yml is not None and self.metadata_validator_yml != "":
            validator_row = RowValidator(self.metadata_validator_yml, self.schema_bronze, self.schema_silver, self.schema_gold, self.spark, self.debug)
            source_data_validated_df = validator_row.validate(self.destination_table_name, source_data_projected_df)
        else:
            source_data_validated_df = source_data_projected_df

        # write the validated data into destination table
        self.save(source_data_validated_df, self.destination_table_full_path, self.destination_table_name, \
                    self.destination_write_mode, self.destination_write_options)

        if self.debug:
            self.logger.debug(f"function end: {function_name}")

