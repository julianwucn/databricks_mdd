#!/usr/bin/env python
# coding: utf-8

# ## datareader_dbf
# 
# New notebook

# In[ ]:


# import
from multiprocessing.dummy import Pool as ThreadPool
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import *
import notebookutils 
import datetime as mdt
import logging

from env.mdd.datareaderfactory import *

from dbfread import DBF

import json


# In[ ]:


# support file format: dbf
# support the read of one file only
# source_options
# {
#    "source_read_options": {}
# }
@DataReaderFactory.register('dbf')
class DBFDataReader(DataReaderBase):

    def __init__(self, source_options, spark, debug = False):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)
        
        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(source_options, spark, debug)

        if debug:
            logger.debug(f"function end: {function_name}")

        self.logger = logger
    # function end: init

    def read_dbf(self, source_path):
        function_name = "read_dbf"

        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_path: {source_path}")

        # get file metadata
        files = notebookutils.fs.ls(source_path)
        file = files[0]
        file_modifytime = file.modifyTime
        if self.debug:
            self.logger.debug(f"file_modifytime: {file_modifytime}")

        _metadata = {"source_path": file.path,
                    "file_name": file.name,
                    "file_size": file.size,
                    "file_block_start": 0,
                    "file_block_length": 0,
                    "file_modification_time": mdt.datetime.fromtimestamp(file_modifytime / 1000).strftime("%Y-%m-%dT%H:%M:%S.%f")
                    }
        _metadata_json = json.dumps(_metadata)
        if self.debug:
            self.logger.debug(f"_metadata: {_metadata}")

        # WARNING: convert the relative file path to "File Path" api which is recognized by dbfread.DBF
        source_path_api = f"/lakehouse/default/{source_path}"
        source_read_options = self.source_options["source_read_options"]
        if self.debug:
            self.logger.debug(f"source_path_api: {source_path_api}")
            self.logger.debug(f"source_read_options: {source_read_options}")

        # read the file
        dbf_table = DBF(source_path_api, \
                        encoding = source_read_options["encoding"], ignorecase = source_read_options["ignorecase"], \
                        lowernames = source_read_options["lowernames"], load = source_read_options["load"], \
                        ignore_missing_memofile = source_read_options["ignore_missing_memofile"], \
                        recfactory = source_read_options["recfactory"])
        if self.debug:
            self.logger.debug(f"dbf_table.loaded: {dbf_table.loaded}")
            self.logger.debug(f"dbf_table.dbversion: {dbf_table.dbversion}")
            self.logger.debug(f"dbf_table.name: {dbf_table.name}")
            self.logger.debug(f"dbf_table.date: {dbf_table.date}")
            self.logger.debug(f"dbf_table.field_names: {dbf_table.field_names}")
            self.logger.debug(f"dbf_table.encoding: {dbf_table.encoding}")
            self.logger.debug(f"dbf_table.filename: {dbf_table.filename}")
            self.logger.debug(f"dbf_table.memofilename: {dbf_table.memofilename}")
            self.logger.debug(f"dbf_table.header: {dbf_table.header}")
            self.logger.debug(f"dbf_table.fields: {dbf_table.fields}")
        
        data_list = []
        for record in dbf_table:
            #record_list = [value[1] for value in record]
            record_list = [value[1].strip() if type(value[1])==str else value[1] for value in record]
            record_list.append("") #_corrupt_data
            record_list.append(_metadata)
            data_list.append(record_list)

    
        if self.debug:
            self.logger.debug(f"function end: {function_name}")

            
        return data_list
    # function end: read

    def read(self, source_path):
        function_name = "read"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_path: {source_path}")

        if type(source_path) != type(list()):
            source_path = [file.strip() for file in source_path.split(",")]

        # read the dbf files in paralell
        paralell_processes = self.source_options["source_read_options"]["max_paralell_processes"]

        if self.debug:
            self.logger.debug(f"paralell_processes: {paralell_processes}")

        if paralell_processes is None:
            pool = ThreadPool()
        else: 
            pool = ThreadPool(paralell_processes)

        results = pool.map(self.read_dbf, source_path)
        pool.close()
        pool.join()

        # combine the lists in results
        data_list = []
        for x in results:
            data_list.extend(x)

        if len(data_list) == 0:
            data_df = None
        else:
            # create dataframe from the list
            source_schema = self.source_options["source_schema"]
            if isinstance(source_schema, str):
                source_schema_new = f"{source_schema}, _metadata MAP<STRING, STRING>"
            else:
                total_columns = len(data_list[0])
                source_schema_new = StructType()
                i = 0
                for x in source_schema:
                    i = i + 1 
                    source_schema_new.add(x)
                    if i == total_columns - 1:
                        break
                source_schema_new.add(StructField("_metadata", MapType(StringType(), StringType()), True))
                
            #print(source_schema_new)
            data_df = self.spark.createDataFrame(data_list, source_schema_new)
        
        if self.debug:
            if data_df is None:
                self.logger.debug(f"data_df: None")
            else:
                self.logger.debug(f"data_df: {data_df.count()}")
        
        if self.debug:
            self.logger.debug(f"function end: {function_name}")

        return data_df

