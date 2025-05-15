#!/usr/bin/env python
# coding: utf-8

# ## dataflow
# 
# New notebook

# In[ ]:


# Use the 2 magic commands below to reload the modules if your module has updates during the current session. You only need to run the commands once.
# %load_ext autoreload
# %autoreload 2
# import
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window

from delta import *
from delta.tables import *
import datetime as mdt
import re
import json
import uuid
import logging
import notebookutils

from inspect import isclass
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module

from env.mdd.metadata import *
from env.mdd.datasyncmanifest import *
from env.mdd.datareaderfactory import *
from env.mdd.datawriter import *
from env.mdd.transformer import *
from env.mdd.patcher import *
from env.mdd.validator import *
from env.mdd.utilhelper import *
import env.mdd.common as common
from env.mdd.utilhelper import *
import env.mdd.mddhelper as mdd

# import all data readers
package_dir = common.datareaders_package_path
module_path_list = package_dir.split("/")
module_path_list.remove(".")
module_path = ".".join(module_path_list)
for (_, module_name, _) in iter_modules([package_dir]):
    module_name = f"{module_path}.{module_name}"
    module = import_module(module_name) 


# In[ ]:


class DataflowHelper:
    def create_dataflow(self, metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, debug = False, metadata_log_yml = None, job_name = "job_123", job_start_timestamp = None, task_name = "task_123", task_start_timestamp = None):
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        # dataflow metadata       
        metadata_dataflow = Metadata_Dataflow(metadata_dataflow_yml, logger, debug) 
        dataflow_type = metadata_dataflow.dataflow_type
        
        if dataflow_type == "onboarding":
            return Onboarding_Dataflow(metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, logger, debug, job_name, job_start_timestamp, task_name, task_start_timestamp)
        elif dataflow_type == "transform" or dataflow_type == "transform_v1":
            return Transform_Dataflow_v1(metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, logger, debug, job_name, job_start_timestamp, task_name, task_start_timestamp)
        elif dataflow_type == "transform_v2":
            return Transform_Dataflow_v2(metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, logger, debug, metadata_log_yml, job_name, job_start_timestamp, task_name, task_start_timestamp)
        else:
            msg = f"dataflow_type: '{dataflow_type}' in metadata '{metadata_dataflow_yml}' is not supported"
            logger.error(msg)
            raise Exception(msg)


# In[ ]:


class Transform_Dataflow_v1:

    def __init__(self, metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, logger, debug = False, job_name = "job_123", job_start_timestamp = None, task_name = "task_123", task_start_timestamp = None):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_environment_yml: {metadata_environment_yml}")
            logger.debug(f"metadata_dataflow_yml: {metadata_dataflow_yml}")
            logger.debug(f"data_sync_options: {data_sync_options}")

        self.spark = spark
        self.debug = debug
        self.logger = logger

        self.job_name = job_name
        self.job_start_timestamp = job_start_timestamp
        self.task_name = task_name
        self.task_start_timestamp = task_start_timestamp

        # environment metadata
        self.metadata_environment = Metadata_Environment(metadata_environment_yml, logger, debug)

        # dataflow metadata       
        self.metadata_dataflow = Metadata_Transform_Dataflow(metadata_dataflow_yml, logger, debug) 
          
        # data sync options
        self.data_sync_options = data_sync_options

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init 
    

    # execute the dataflow 
    def run(self):
        function_name = "run"
        sync_start_timestamp = mdt.datetime.now()
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
        
        m_en = self.metadata_environment
        m_df = self.metadata_dataflow

        # map schema holder to schema
        map_table_schema = lambda x: x.replace("<bronze>", m_en.destination_bronze_schema) \
                                    .replace("<silver>", m_en.destination_silver_schema) \
                                    .replace("<gold>", m_en.destination_gold_schema)

        # get source table name
        source_lakehouse = m_en.destination_lakehouse
        source_table_schema = map_table_schema(m_df.source_table_schema)
        source_table_name = f"{source_table_schema}.{m_df.source_table_name}"
        source_table_full_name = f"{source_lakehouse}.{source_table_schema}.{m_df.source_table_name}"
        if self.debug:
            self.logger.debug(f"source_table_name: {source_table_name}")
            self.logger.debug(f"source_table_full_name: {source_table_full_name}")
        
        # get destination table name
        destination_lakehouse = m_en.destination_lakehouse
        destination_table_schema = map_table_schema(m_df.destination_table_schema)
        destination_table_name = f"{destination_table_schema}.{m_df.destination_table_name}"
        destination_table_full_name = f"{destination_lakehouse}.{destination_table_schema}.{m_df.destination_table_name}"
        destination_table_full_path = f"/{m_en.destination_lakehouse_guid}/Tables/{destination_table_schema}/{m_df.destination_table_name}"
        destination_write_mode = m_df.destination_write_mode
        metadata_validator_yml = m_df.validators
        destination_projected_sql = m_df.destination_projected_sql
        destination_write_options = m_df.destination_write_options
        if self.debug:
            self.logger.debug(f"destination_table_name: {destination_table_name}")
            self.logger.debug(f"destination_table_full_name: {destination_table_full_name}")
            self.logger.debug(f"destination_table_full_path: {destination_table_full_path}")
            self.logger.debug(f"destination_write_mode: {destination_write_mode}")
            self.logger.debug(f"metadata_validator_yml: {metadata_validator_yml}")
            self.logger.debug(f"destination_projected_sql: {destination_projected_sql}")
            self.logger.debug(f"destination_write_options: {destination_write_options}")

        # register the main source table
        property_name = "mdd.source.primary"
        tableutil = TableUtil(destination_table_name, self.spark, self.debug)
        tableutil.set_table_property_list(property_name, source_table_name)

        # run the pre_transform_script
        pre_transform_script = m_df.pre_transform_script  
        if not (pre_transform_script is None or pre_transform_script == ""):
            self.logger.info(f"pre transform script start")
            self.spark.sql(pre_transform_script)
            self.logger.info(f"pre transform script end")

        # loop the file groups, read, transform and write the data to destination
        transformers = m_df.transformers
        if self.debug:
            self.logger.debug(f"transformers: {transformers}")

        self.logger.info(f"data sync start: {source_table_name} -> {destination_table_name}")

        # get the new data
        self.logger.info(f"read start")
        read_start_timestamp = mdt.datetime.now()

        # get final data sync options generate by the data sync manifest
        data_sync_manifest = TableDataSyncManifest(self.spark, self.debug)
        df_batches = data_sync_manifest.get_data_sync_manifest(source_table_name, destination_table_name, self.data_sync_options)
        df_source = data_sync_manifest.source_data_df

        data_sync_mode = self.data_sync_options["data_sync_mode"]
        data_sync_mode_final = data_sync_manifest.data_sync_mode_final
        if data_sync_mode != data_sync_mode_final:
            data_sync_mode_final = f"{data_sync_mode} -> {data_sync_mode_final}"
            msg = f"data sync mode fallback: {data_sync_mode_final}"
            self.logger.warning(msg)  

        rows_read = df_source.count()
        self.logger.info(f"rows read: {rows_read}")
        self.logger.info(f"read end")
        read_end_timestamp = mdt.datetime.now()

        batches_total = len(df_batches)
        if batches_total == 0:
            self.logger.warning("skipped with no data")

        if len(df_batches) > 1:
            self.logger.info(f"rows per batch: {self.data_sync_options['rows_per_batch']}")
            self.logger.info(f"total batches: {len(df_batches)}")
        
        schema_bronze = m_en.destination_bronze_schema
        schema_silver = m_en.destination_silver_schema
        schema_gold = m_en.destination_gold_schema

        batch_i = 0
        for batch in df_batches[:]:
            batch_i += 1

            rows_in_batch = rows_read
            batch_detail = f"{batch_i}/{batches_total}: {batch[0].strftime('%Y-%m-%d %H:%M:%S.%f')} - {batch[1].strftime('%Y-%m-%d %H:%M:%S.%f')}"

            if len(df_batches) == 1:
                df_batch = df_source
            else:
                df_batch = df_source.filter((df_source._commit_timestamp >= batch[0]) & (df_source._commit_timestamp <= batch[1]))
                rows_in_batch = df_batch.count()
                self.logger.info(f"process batch {batch_detail}")
                self.logger.info(f"rows read: {rows_in_batch}")

                # skip and warning if current batch has no data
                if df_batch.isEmpty():
                    self.logger.warning(f"batch {batch_i} has no data: {batch[0]} - {batch[1]}")
                    continue

            # transform
            self.logger.info(f"transform start")
            transform_start_timestamp = mdt.datetime.now()

            transformerhelper = TransformerHelper(destination_table_name, df_batch, transformers, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            df_transformed = transformerhelper.run()

            transform_end_timestamp = mdt.datetime.now()
            self.logger.info(f"transform end")

            if df_transformed is None:
                msg = f"transform returns None"
                self.logger.error(f"transform returns None")
                raise Exception(msg)
            if df_transformed.isEmpty():
                self.logger.warning(f"transform returns no data")
                continue
            
            # add the source table as _source_name
            df_transformed = df_transformed.withColumn("_source_name", lit(source_table_name))

            # write
            write_start_timestamp = mdt.datetime.now()
            datawriter = TransformDataWriter(df_transformed, destination_table_full_path, destination_table_name, destination_write_mode, \
                destination_write_options, destination_projected_sql, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            datawriter.write()      
            rows_inserted = datawriter.rows_inserted
            rows_updated = datawriter.rows_updated
            rows_deleted = datawriter.rows_deleted
            write_end_timestamp = mdt.datetime.now() 

            # log sync information
            table_run_id = f"{destination_table_name}_{sync_start_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}"
            task_run_id = f"{self.task_name}_{self.task_start_timestamp}"
            job_run_id = f"{self.job_name}_{self.job_start_timestamp}"
            source_table_timestamp = df_batch.select(max("_record_timestamp")).collect()[0][0]
            source_table_cfversion = df_batch.select(max("_commit_version")).collect()[0][0]
            table_run_timestamp = mdt.datetime.now() 
            mdd.etl_job_tables_update(    
                table_run_id              
                ,destination_table_name               
                ,data_sync_mode_final 
                ,destination_write_mode          
                ,source_table_name        
                ,source_table_timestamp   
                ,source_table_cfversion   
                ,rows_read                
                ,batches_total                  
                ,batch_detail                    
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
                ,table_run_timestamp            
            )
        # end for

        self.logger.info(f"data sync end: {source_table_name} -> {destination_table_name}")

        # run table validators
        if metadata_validator_yml is not None and metadata_validator_yml != "":
            validator_table = TableValidator(metadata_validator_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            validator_table.validate(destination_table_name, destination_table_full_path)

        # run data patchers
        metadata_patcher_yml = m_df.patchers
        if metadata_patcher_yml is not None and metadata_patcher_yml != "":
            patcher_helper = PatcherHelper(destination_table_full_path, destination_table_name, metadata_patcher_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            patcher_helper.run()

        # run the post_transform_script
        post_transform_script = m_df.post_transform_script  
        if not (post_transform_script is None or post_transform_script == ""):
            self.logger.info(f"post transform script start")
            self.spark.sql(post_transform_script)
            self.logger.info(f"post transform script end")

        if self.debug: 
            self.logger.debug(f"function end: {function_name}")
    # function end: run


# In[ ]:


class Transform_Dataflow_v2:

    def __init__(self, metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, logger, debug = False, metadata_log_yml = None, job_name = "job_123", job_start_timestamp = None, task_name = "task_123", task_start_timestamp = None):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_environment_yml: {metadata_environment_yml}")
            logger.debug(f"metadata_dataflow_yml: {metadata_dataflow_yml}")
            logger.debug(f"data_sync_options: {data_sync_options}")

        self.spark = spark
        self.debug = debug
        self.logger = logger
        self.metadata_log_yml = metadata_log_yml
        self.job_name = job_name
        self.job_start_timestamp = job_start_timestamp
        self.task_name = task_name
        self.task_start_timestamp = task_start_timestamp

        # environment metadata
        self.metadata_environment = Metadata_Environment(metadata_environment_yml, logger, debug)

        # dataflow metadata       
        self.metadata_dataflow = Metadata_Transform_Dataflow(metadata_dataflow_yml, logger, debug) 
          
        # data sync options
        self.data_sync_options = data_sync_options

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init 
    

    # execute the dataflow 
    def run(self):
        function_name = "run"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
        
        m_en = self.metadata_environment
        m_df = self.metadata_dataflow

        # map schema holder to schema
        map_table_schema = lambda x: x.replace("<bronze>", m_en.destination_bronze_schema) \
                                    .replace("<silver>", m_en.destination_silver_schema) \
                                    .replace("<gold>", m_en.destination_gold_schema)

        # get source table name
        source_lakehouse = m_en.destination_lakehouse
        source_table_schema = map_table_schema(m_df.source_table_schema)
        source_table_name = f"{source_table_schema}.{m_df.source_table_name}"
        source_table_full_name = f"{source_lakehouse}.{source_table_schema}.{m_df.source_table_name}"
        if self.debug:
            self.logger.debug(f"source_table_name: {source_table_name}")
            self.logger.debug(f"source_table_full_name: {source_table_full_name}")
        
        # get destination table name
        destination_lakehouse = m_en.destination_lakehouse
        schema_bronze = m_en.destination_bronze_schema
        schema_silver = m_en.destination_silver_schema
        schema_gold = m_en.destination_gold_schema

        destination_table_schema = map_table_schema(m_df.destination_table_schema)
        destination_table_name = f"{destination_table_schema}.{m_df.destination_table_name}"
        destination_table_full_name = f"{destination_lakehouse}.{destination_table_schema}.{m_df.destination_table_name}"
        destination_table_full_path = f"/{m_en.destination_lakehouse_guid}/Tables/{destination_table_schema}/{m_df.destination_table_name}"
        destination_write_mode = m_df.destination_write_mode
        metadata_validator_yml = m_df.validators
        destination_projected_sql = m_df.destination_projected_sql
        destination_write_options = m_df.destination_write_options
        if self.debug:
            self.logger.debug(f"destination_table_name: {destination_table_name}")
            self.logger.debug(f"destination_table_full_name: {destination_table_full_name}")
            self.logger.debug(f"destination_table_full_path: {destination_table_full_path}")
            self.logger.debug(f"destination_write_mode: {destination_write_mode}")
            self.logger.debug(f"metadata_validator_yml: {metadata_validator_yml}")
            self.logger.debug(f"destination_projected_sql: {destination_projected_sql}")
            self.logger.debug(f"destination_write_options: {destination_write_options}")

        # register the main source table
        property_name = "mdd.source.primary"
        tableutil = TableUtil(destination_table_name, self.spark, self.debug)
        tableutil.set_table_property_list(property_name, source_table_name)

        # prepare tranform params
        params = {
            "lakehouse_name": destination_lakehouse
            ,"lakehouse_id": m_en.destination_lakehouse_guid
            ,"schema_bronze": schema_bronze
            ,"schema_silver": schema_silver
            ,"schema_gold": schema_gold
            ,"log_metadata": self.metadata_log_yml

            ,"source_table_schema": source_table_schema
            ,"source_table_name": m_df.source_table_name
            ,"destination_table_schema": destination_table_schema
            ,"destination_table_name": m_df.destination_table_name
            ,"destination_projected_sql": destination_projected_sql
            ,"destination_write_mode": destination_write_mode
            ,"destination_write_options": destination_write_options
            ,"destination_projected_sql": destination_projected_sql
            ,"data_sync_options": self.data_sync_options

            ,"validators_metadata": metadata_validator_yml
            ,"pre_transform_script": m_df.pre_transform_script
            ,"post_transform_script": m_df.post_transform_script
            ,"debug": self.debug

            ,"job_name": self.job_name
            ,"job_start_timestamp": self.job_start_timestamp if type(self.job_start_timestamp) == str else self.job_start_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
            ,"task_name": self.task_name
            ,"task_start_timestamp": self.task_start_timestamp if type(self.task_start_timestamp) == str else self.task_start_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f') 
        }  

        params_json = json.dumps(params)

        try:
            for transformer in m_df.transformers:
                if transformer["active"] == True and transformer["transformer_type"] == "notebook":
                    transform_notebook_path = transformer["transformer_script"]
                    transform_notebook = transform_notebook_path[transform_notebook_path.rfind("/")+1:]
                    timeoutSeconds = 60*60*12
                    notebookutils.notebook.run(transform_notebook, timeoutSeconds, {"params": params_json})

                    # only run the 1st active notebook
                    break
        except Exception as error:
            self.logger.error(f"run notebook failed: {transform_notebook}")
            raise error

        # run table validators
        if metadata_validator_yml is not None and metadata_validator_yml != "":
            validator_table = TableValidator(metadata_validator_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            validator_table.validate(destination_table_name, destination_table_full_path)

        # run data patchers
        metadata_patcher_yml = m_df.patchers
        if metadata_patcher_yml is not None and metadata_patcher_yml != "":
            patcher_helper = PatcherHelper(destination_table_full_path, destination_table_name, metadata_patcher_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            patcher_helper.run()

        if self.debug: 
            self.logger.debug(f"function end: {function_name}")
    # function end: run


# In[ ]:


class Onboarding_Dataflow:

    def __init__(self, metadata_dataflow_yml, metadata_environment_yml, data_sync_options, spark, logger, debug = False, job_name = "job_123", job_start_timestamp = None, task_name = "task_123", task_start_timestamp = None):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_environment_yml: {metadata_environment_yml}")
            logger.debug(f"metadata_dataflow_yml: {metadata_dataflow_yml}")
            logger.debug(f"data_sync_options: {data_sync_options}")

        self.spark = spark
        self.debug = debug
        self.logger = logger

        self.job_name = job_name
        self.job_start_timestamp = job_start_timestamp
        self.task_name = task_name
        self.task_start_timestamp = task_start_timestamp

        # environment metadata
        self.metadata_environment = Metadata_Environment(metadata_environment_yml, debug)

        # dataflow metadata       
        self.metadata_dataflow = Metadata_Onboarding_Dataflow(metadata_dataflow_yml, debug) 
          
        # data sync options
        self.data_sync_options = data_sync_options    

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init 
    

    # execute the dataflow 
    def run(self):
        function_name = "run"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        m_df = self.metadata_dataflow
        m_en = self.metadata_environment


        # reader metadata
        source_data_format = m_df.source_data_format
        source_data_path = f"{m_en.source_data_root_path}{m_df.source_data_relative_path}"
        source_file_name_regex = m_df.source_file_name_regex
        source_data_read_options = m_df.Source_data_read_options
        source_data_schema = m_df.source_data_schema

        # writer metadata
        destination_lakehouse = m_en.destination_lakehouse
        destination_lakehouse_guid = m_en.destination_lakehouse_guid

        destination_table_schema = m_df.destination_table_schema
        if destination_table_schema is None or destination_table_schema == "":
            destination_table_schema = m_en.destination_bronze_schema

        destination_projected_sql = m_df.destination_projected_sql
        destination_write_mode = m_df.destination_write_mode
        destination_write_options = m_df.destination_write_options
        
        # validator metadata
        metadata_validator_yml = m_df.validators


        # prepare local variables
        destination_table_name = f"{destination_table_schema}.{m_df.destination_table_name}"
        destination_table_full_name = f"{m_en.destination_lakehouse}.{destination_table_schema}.{m_df.destination_table_name}"
        destination_table_full_path = f"/{m_en.destination_lakehouse_guid}/Tables/{destination_table_schema}/{m_df.destination_table_name}"
        if self.debug:
            self.logger.debug(f"destination_table_full_name: {destination_table_full_name}")
            self.logger.debug(f"destination_table_full_path: {destination_table_full_path}")

        # run the pre_onboard_script
        pre_onboard_script = m_df.pre_onboard_script  
        if not (pre_onboard_script is None or pre_onboard_script == ""):
            self.logger.info(f"pre onboard script start")
            self.spark.sql(pre_onboard_script)
            self.logger.info(f"pre onboard script end")

        # check destination table 
        if not self.spark.catalog.tableExists(destination_table_full_name):
            msg = f"table {destination_table_full_name} does not exist, please create the table."
            self.logger.error(msg)
            raise Exception(msg)

        # use the schema of the destination table if source data schema is not defined
        if source_data_schema is None or source_data_schema == "":
            source_data_schema = self.spark.read.table(destination_table_full_name).schema
            if self.debug:
                self.logger.debug(f"warning: source_data_schema not defined, use destination table schema instead")            
        else:
            source_data_schema = source_data_schema + ", _corrupt_data string"


        # get the loaded files list
        destination_files_df = self.spark.sql(f"select distinct file_name, file_timestamp from mdd.etl_job_files where table_name = '{destination_table_name}'")
        destination_files = list((row["file_name"], row["file_timestamp"]) for row in destination_files_df.collect())
        if self.debug:
            self.logger.debug(f"destination_files_df: {destination_files_df.count()}")
            display(destination_files_df.sort(destination_files_df.file_name.desc()))
            self.logger.debug(f"destination_files: {destination_files}")

        # get the new files
        data_sync_manifest = FileDataSyncManifest(self.spark, self.debug)
        data_sync_manifest_group_list = data_sync_manifest.get_data_sync_manifest(source_data_path, source_file_name_regex, destination_files, self.data_sync_options)
        data_sync_manifest_files = data_sync_manifest.data_sync_manifest_files
        data_sync_manifest_df = data_sync_manifest.data_sync_manifest_df

        # loop the file groups, read the data and write to destination
        self.logger.info(f"{m_df.dataflow_type} start: {destination_table_name}") 
        
        self.logger.info(f"files path: {source_data_path}")
        self.logger.info(f"files in total: {data_sync_manifest_files}")
        if data_sync_manifest_files > 0:
            self.logger.info(f"files per batch: {self.data_sync_options['files_per_batch']}")
            self.logger.info(f"batches in total: {len(data_sync_manifest_group_list)}")

        schema_bronze = destination_table_schema
        schema_silver = m_en.destination_silver_schema
        schema_gold = m_en.destination_gold_schema
        i = 0
        for file_path_list in data_sync_manifest_group_list[:]:
            i += 1
            self.logger.info(f"batch #{i} start")
            self.logger.info(f"files in batch: {len(file_path_list)}")

            source_options = {
                    "source_format": source_data_format,
                    "source_read_options": source_data_read_options,
                    "source_schema":source_data_schema
                }
            read_start_timestamp = mdt.datetime.utcnow()
            datareader = DataReaderFactory.create_datareader(source_data_format, source_options, self.spark, self.debug)
            if datareader is None:
                err_msg = f"the data reader for '{source_data_format}' is not registered"
                self.logger.error(err_msg)
                raise Exception(err_msg)

            self.logger.info(f"read start")
            batch_data_df = datareader.read(file_path_list)
            if batch_data_df is not None:
                self.logger.info(f"read rows in total: {batch_data_df.count()}")
            read_end_timestamp = mdt.datetime.utcnow()

            self.logger.info(f"write start")
            write_start_timestamp = mdt.datetime.utcnow()
            write_end_timestamp = mdt.datetime.utcnow()
            imported_files_df = data_sync_manifest_df.where(data_sync_manifest_df.file_path.isin(file_path_list)) 
                                                                                
            if batch_data_df is not None:
                # delete the existing data if the file name and file modification timestamp is exactly the same
                # if the file name is the same, but the file modification timestamp is different, then keep the existing data
                # to-do: could mark them as soft-deleted
                dt_dest = DeltaTable.forPath(self.spark, destination_table_full_path)
                (
                    dt_dest.alias("t") 
                        .merge(source = imported_files_df.alias("s"), condition = "t._source_name = s.file_name and t._source_timestamp = s.file_modification_timestamp") 
                        .whenMatchedDelete()
                        .execute()
                )
                
                # write the validated data into destination table
                datawriter = OnboardDataWriter(batch_data_df, destination_table_full_path, destination_table_name, destination_write_mode, \
                    destination_write_options, destination_projected_sql, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
                datawriter.write()
                write_end_timestamp = mdt.datetime.utcnow()

                total_rows_df = batch_data_df.withColumns({"file_name": "_metadata.file_name", "file_modification_timestamp": "_metadata.file_modification_time"}) \
                                        .groupBy("file_name", "file_modification_timestamp")\
                                        .agg(count("*").alias("rows_read"))

                total_imported_sql = f"""
                                select 
                                    _source_name as file_name
                                    ,_source_timestamp as file_modification_timestamp
                                    ,count(*) as rows_onboarded
                                    ,sum(case when _corrupt_data is not null and _corrupt_data <> '' then 1 else 0 end) as rows_corrupted
                                    ,sum(case when _deleted_by_source then 1 else 0 end) as rows_deleted_by_source
                                    ,sum(case when _deleted_by_validation then 1 else 0 end) as rows_deleted_by_validation
                                from {destination_table_name}
                                where _record_timestamp >= '{write_start_timestamp}'
                                group by _source_name, _source_timestamp"""
                total_imported_rows_df = self.spark.sql(total_imported_sql)

                imported_files_df = imported_files_df.alias("t1").join(total_rows_df.alias("t2"), ["file_name", "file_modification_timestamp"], "left") \
                                        .join(total_imported_rows_df.alias("t3"), ["file_name", "file_modification_timestamp"], "left") \
                                        .select("t1.*", "t2.rows_read", "t3.rows_onboarded", "t3.rows_corrupted", "t3.rows_deleted_by_source", "t3.rows_deleted_by_validation") \
                                        .fillna({"rows_read": 0}).fillna({"rows_onboarded": 0}).fillna({"rows_corrupted": 0}).fillna({"rows_deleted_by_source": 0}).fillna({"rows_deleted_by_validation": 0})
            else:
                imported_files_df = imported_files_df.withColumn("rows_read", lit(long(0))).withColumn("rows_onboarded", lit(long(0))).withColumn("rows_corrupted", lit(long(0))) \
                                                        .withColumn("rows_deleted_by_source", lit(long(0))).withColumn("rows_deleted_by_validation", lit(long(0)))

            # save the state of the imported files with timestamp
            #imported_files_df.printSchema()
            job_run_id = f"{self.job_name}_{self.job_start_timestamp}"
            task_run_id = f"{self.task_name}_{self.task_start_timestamp}"
            imported_files_df = (
                imported_files_df.withColumn("table_name", lit(destination_table_name))
                .withColumn("log_timestamp", lit(current_timestamp()))
                .withColumn("data_sync_mode", lit(self.data_sync_options["data_sync_mode"]))
                .withColumn("data_write_mode", lit(destination_write_mode))
                .withColumn("read_start_timestamp", lit(read_start_timestamp))
                .withColumn("read_end_timestamp", lit(read_end_timestamp))
                .withColumn("write_start_timestamp", lit(write_start_timestamp))
                .withColumn("write_end_timestamp", lit(write_end_timestamp))
                .withColumn("job_run_id", lit(job_run_id))
                .withColumn("task_run_id", lit(task_run_id))
                .selectExpr(
                    "concat(file_name, '_', cast(file_modification_timestamp as string)) as file_run_id",
                    "file_name",
                    "file_modification_timestamp as file_timestamp",
                    "file_date",
                    "file_path",
                    "table_name",
                    "data_sync_mode",
                    "data_write_mode",
                    "rows_read",
                    "rows_onboarded",
                    "rows_corrupted",
                    "rows_deleted_by_source",
                    "rows_deleted_by_validation",
                    "read_start_timestamp",
                    "read_end_timestamp",
                    "write_start_timestamp",
                    "write_end_timestamp",
                    "task_run_id",
                    "job_run_id",
                    "log_timestamp"
                )
            ) 
                           
            imported_files_df.write.mode("append").option("mergeSchema", "true").saveAsTable("mdd.etl_job_files")
            

        # end for

        # run table validators
        self.logger.info(f"table validation start")
        if metadata_validator_yml is not None and metadata_validator_yml != "":
            validator_table = TableValidator(metadata_validator_yml, schema_bronze, schema_silver, schema_gold, self.spark, self.debug)
            validator_table.validate(destination_table_name, destination_table_full_path)
        self.logger.info(f"table validation end")

        # run the pre_onboard_script
        post_onboard_script = m_df.post_onboard_script  
        if not (post_onboard_script is None or post_onboard_script == ""):
            self.logger.info(f"post onboard script start")
            self.spark.sql(post_onboard_script)
            self.logger.info(f"post onboard script end")

        self.logger.info(f"{m_df.dataflow_type} end: {destination_table_name}") 
        if self.debug:
            self.logger.debug(f"function end: {function_name}")
    # function end: run

