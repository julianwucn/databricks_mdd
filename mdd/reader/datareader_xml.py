#!/usr/bin/env python
# coding: utf-8

# ## datareader_xml
# 
# New notebook

# In[ ]:


from pyspark.sql.functions import *
import notebookutils 
import datetime as mdt
import logging
import json

from env.mdd.datareaderfactory import *


# In[ ]:


@DataReaderFactory.register('xml')
class xmlDataReader(DataReaderBase):

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

        # xml reader only takes the path of str, not list
        if type(source_path) == type(list()):
            source_path = ",".join(source_path)
        
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

        data_df = data_df.load(source_path).withColumn("_file_path", input_file_name())

        # take one file or one path only
        if type(source_path) == str:
            source_path = [file.strip() for file in source_path.split(",")]
        source_files = []
        for file_path in source_path:
            files = notebookutils.fs.ls(file_path)
            for file_info in files:
                if file_info.isFile:
                    source_files.append({"_file_name": file_info.name, 
                            "_file_path": file_info.path, 
                            "_file_modification_timestamp": mdt.datetime.fromtimestamp(file_info.modifyTime / 1000).strftime("%Y-%m-%d %H:%M:%S.%f"), 
                            "_file_size": file_info.size})
        #print(f"source_files: {source_files}")
        files_df = self.spark.createDataFrame(source_files, "_file_name string, _file_path string, _file_modification_timestamp string, _file_size bigint") \
                        .withColumn("_metadata"
                            ,create_map(
                                    lit("file_name"), col("_file_name"),
                                    lit("file_size"), col("_file_size"),
                                    lit("file_block_start"), lit(0),
                                    lit("file_block_length"), lit(0),
                                    lit("file_modification_time"), col("_file_modification_timestamp"))
                            )
        #files_df.show(truncate = False)

        data_df = data_df.alias("s").join(files_df.alias("t"), col("s._file_path") == col("t._file_path"), "left") \
                .select("s.*", "t._metadata").drop("_file_path")


        if self.debug:
            self.logger.debug(f"data_df: {data_df.count()}")
            self.logger.debug(f"data_df: {data_df.schema}")
            self.logger.debug(f"function end: {function_name}")

        return data_df
    # function end: read

