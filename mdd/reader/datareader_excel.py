#!/usr/bin/env python
# coding: utf-8

# ## datareader_excel
# 
# New notebook

# In[1]:


from multiprocessing.dummy import Pool as ThreadPool
from pyspark.sql.functions import col, lit, udf, when, create_map
import notebookutils 
import datetime as mdt
import logging
import json
import pandas as pd

from env.mdd.datareaderfactory import *


# In[ ]:


@DataReaderFactory.register('xlsx')
@DataReaderFactory.register('xls')
@DataReaderFactory.register('xlsm')
@DataReaderFactory.register('xlsb')
@DataReaderFactory.register('odf')
@DataReaderFactory.register('ods')
@DataReaderFactory.register('odt')
class ExcelDataReader(DataReaderBase):

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

    def read_excel(self, source_path):
        function_name = "read_excel"

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

        # WARNING: convert the relative file path to "File Path" api which is recognized by pandas.read_csv()
        source_path_api = f"/lakehouse/default/{source_path}"
        source_read_options = self.source_options["source_read_options"]
        source_data_schema = self.source_options["source_schema"]
        if self.debug:
            self.logger.debug(f"source_path_api: {source_path_api}")
            self.logger.debug(f"source_read_options: {source_read_options}")
            self.logger.debug(f"source_data_schema: {source_data_schema}")

        # read the file
        df_pd = pd.read_excel(source_path_api \
                            ,sheet_name      = source_read_options['sheet_name'     ]
                            ,header          = source_read_options['header'         ]
                            ,names           = source_read_options['names'          ]
                            ,index_col       = source_read_options['index_col'      ]
                            ,usecols         = source_read_options['usecols'        ]
                            ,engine          = source_read_options['engine'         ]
                            ,converters      = source_read_options['converters'     ]
                            ,true_values     = source_read_options['true_values'    ]
                            ,false_values    = source_read_options['false_values'   ]
                            ,skiprows        = source_read_options['skiprows'       ]
                            ,nrows           = source_read_options['nrows'          ]
                            ,na_values       = source_read_options['na_values'      ]
                            ,keep_default_na = source_read_options['keep_default_na']
                            ,na_filter       = source_read_options['na_filter'      ]
                            ,verbose         = source_read_options['verbose'        ]
                            ,parse_dates     = source_read_options['parse_dates'    ]
                            ,date_format     = source_read_options['date_format'    ]
                            ,thousands       = source_read_options['thousands'      ]
                            ,decimal         = source_read_options['decimal'        ]
                            ,comment         = source_read_options['comment'        ]
                            ,skipfooter      = source_read_options['skipfooter'     ]
                            ,storage_options = source_read_options['storage_options']
                            ,dtype_backend   = source_read_options['dtype_backend'  ]
                            ,dtype           = source_read_options['dtype'          ]
                        )
        df_pd["_corrupt_data"] = ""
        if self.debug:
            self.logger.debug(f"df_pd.dtypes: {df_pd.dtypes}")
        
        if source_data_schema is None:
            data_df = self.spark.createDataFrame(df_pd)
        else:
            data_df = self.spark.createDataFrame(df_pd, source_data_schema)

        data_df = data_df.withColumn("_metadata_source_path", lit(_metadata["source_path"])) \
                        .withColumn("_metadata_file_name", lit(_metadata["file_name"])) \
                        .withColumn("_metadata_file_size", lit(_metadata["file_size"])) \
                        .withColumn("_metadata_file_block_start", lit(_metadata["file_block_start"])) \
                        .withColumn("_metadata_file_block_length", lit(_metadata["file_block_length"])) \
                        .withColumn("_metadata_file_modification_time", lit(_metadata["file_modification_time"])) 

        data_df = data_df.withColumn("_metadata"
                ,create_map(
                        lit("source_path"), col("_metadata_source_path"),
                        lit("file_name"), col("_metadata_file_name"),
                        lit("file_size"), col("_metadata_file_size"),
                        lit("file_block_start"), col("_metadata_file_block_start"),
                        lit("file_block_length"), col("_metadata_file_block_length"),
                        lit("file_modification_time"), col("_metadata_file_modification_time"))
                )
                
        data_df = data_df.drop("_metadata_source_path", "_metadata_file_name", "_metadata_file_size", "_metadata_file_block_start", "_metadata_file_block_length", "_metadata_file_modification_time")

        #data_df.printSchema()

        if self.debug:
            self.logger.debug(f"data_df.schema: {data_df.schema.simpleString()}")
        

        if self.debug:
            self.logger.debug(f"function end: {function_name}")

            
        return data_df
    # function end: read

    def read(self, source_path):
        function_name = "read"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"source_path: {source_path}")

        if type(source_path) != type(list()):
            source_path = [file.strip() for file in source_path.split(",")]

        # read the dbf files in paralell
        max_paralell_processes = self.source_options["source_read_options"]["max_paralell_processes"]
        paralell_processes = len(source_path)
        if paralell_processes > max_paralell_processes:
            paralell_processes = max_paralell_processes

        if self.debug:
            self.logger.debug(f"paralell_processes: {paralell_processes}")

        pool = ThreadPool(paralell_processes)
        results = pool.map(self.read_excel, source_path)

        pool.close()
        pool.join()

        data_df = None
        for df in results:
            if df is not None:
                if data_df is None:
                    data_df = df
                else:
                    data_df = data_df.unionByName(df)
        
        if self.debug:
            if data_df is None:
                self.logger.debug(f"data_df: None")
            else:
                self.logger.debug(f"data_df: {data_df.count()}")
        
        if self.debug:
            self.logger.debug(f"function end: {function_name}")

        return data_df

