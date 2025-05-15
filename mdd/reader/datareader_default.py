#!/usr/bin/env python
# coding: utf-8

# ## datareader_default
# 
# New notebook

# In[ ]:


# import
from pyspark.sql.functions import col, lit
import notebookutils 
import datetime as mdt
import logging

from env.mdd.datareaderfactory import *


# In[1]:


# support file format: csv, text, json, orc, parquet, table, jdbc
# support the read of multiple files or file path
# source_options
# {
#    "source_format": "csv",
#    "source_read_options": {},
#    "source_schema"：""
# }
@DataReaderFactory.register('csv')
@DataReaderFactory.register('text')
@DataReaderFactory.register('json')
@DataReaderFactory.register('orc')
@DataReaderFactory.register('parquet')
@DataReaderFactory.register('table')
@DataReaderFactory.register('jdbc')
class DefaultDataReader(DataReaderBase):
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

    def read(self, source_path):
        function_name = "read"

        if type(source_path) != type(list()):
            source_path = [file.strip() for file in source_path.split(",")]
        
        source_format = self.source_options["source_format"]
        source_read_options = self.source_options["source_read_options"]
        key = "source_schema"
        source_schema = None
        if key in self.source_options:
            source_schema = self.source_options[key] 

        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_path: {source_path}")
            self.logger.debug(f"source_read_options: {source_read_options}")
            self.logger.debug(f"source_schema: '{source_schema}'")

        # read the file(s)
        data_df = self.spark.read.format(source_format).options(**source_read_options)
        if not (source_schema is None or source_schema == ""):
            data_df = data_df.schema(source_schema) 

        # add the file's metadata which including file name, path, size, modification timestamp.
        data_df = data_df.load(source_path).withColumn("_metadata", col("_metadata"))     

        if self.debug:
            self.logger.debug(f"data_df: {data_df.count()}")
            data_df.printSchema()
            self.logger.debug(f"function end: {function_name}")

        return data_df
    # function end: read

