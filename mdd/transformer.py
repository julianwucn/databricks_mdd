#!/usr/bin/env python
# coding: utf-8

# ## transformer
# 
# New notebook

# In[ ]:


from abc import abstractmethod, ABC
from inspect import getmembers, isclass, isabstract
import sys
import importlib.util
import re
import os
import uuid
import logging

from env.mdd.common import *
from env.mdd.utilhelper import *


# In[ ]:


class TransformerAbs(ABC):
    def __init__(self, schema_bronze, schema_silver, schema_gold, spark, logger, debug):
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold
        
        self.spark = spark
        self.logger = logger
        self.debug = debug

    @abstractmethod
    def transform(self, data_df):
        pass


# In[ ]:


class TransformerFactory:

    def __init__(self, schema_bronze, schema_silver, schema_gold, spark, logger, debug):
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold

        self.spark = spark
        self.logger = logger
        self.debug = debug

    def get_transformer(self, transformer_name, transformer_path):
        function_name = "get_transformer"
        module_file = transformer_path
        module_name = os.path.basename(module_file).replace(".py", "")

        if self.debug:
            self.logger.debug(f"module_file: {module_file}")
            self.logger.debug(f"module_name: {module_name}")

        module_spec = importlib.util.spec_from_file_location(module_name, module_file)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)

        transformer = None
        classes = getmembers(module)
        for name, _type in classes:
            if isclass(_type) and not isabstract(_type) and issubclass(_type, TransformerAbs) and name == transformer_name:
                transformer = _type(self.schema_bronze, self.schema_silver, self.schema_gold, self.spark, self.logger, self.debug)
                break

        if transformer is None:
            msg = f"The transformer class '{transformer_name}' is not found in '{transformer_path}'"
            self.logger.error(msg)
            raise Exception(msg)

        return transformer



                


# In[ ]:


class TransformerHelper:
    def __init__(self, destination_table_schema_name, source_data_df, transformers, schema_bronze, schema_silver, schema_gold, spark, debug):
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        self.destination_table_schema_name = destination_table_schema_name
        self.source_data_df = source_data_df
        self.transformers = transformers
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold

        self.spark = spark
        self.logger = logger
        self.debug = debug

        if debug:
            self.logger.debug(f"transformers: {transformers}")

    def run(self):
        function_name = "run"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        if self.transformers is None or self.transformers == "":
            self.transformers = []

        transformed_data_df = self.source_data_df
        if self.debug:
            self.logger.debug(f"data before transform: {transformed_data_df.count()}")

        tableutil = TableUtil(self.destination_table_schema_name, self.spark, self.debug)

        for transformer in self.transformers:

            transformer_name = transformer["transformer"]
            transformer_type = transformer["transformer_type"]
            transformer_script = transformer["transformer_script"]
            transformer_secondary_tables = transformer["transformer_secondary_tables"]
            transformer_active = transformer["active"]
            
            if self.debug:
                self.logger.debug(f"transformer: {transformer_name}")
                self.logger.debug(f"transformer_type: {transformer_type}")
                self.logger.debug(f"transformer_script: {transformer_script}")
                self.logger.debug(f"transformer_secondary_tables: {transformer_secondary_tables}")
                self.logger.debug(f"Active: {transformer_active}")

            if not transformer_active:
                continue

            # register the sencondary tables
            if transformer_secondary_tables is not None:
                property_name = "mdd.source.secondary"
                transformer_secondary_tables = transformer_secondary_tables.replace("<bronze>", self.schema_bronze).replace("<silver>", self.schema_silver).replace("<gold>", self.schema_gold)
                tableutil.set_table_property_list(property_name, transformer_secondary_tables)
            
            self.logger.info(f"transform: {transformer_name}")


            # transform
            tranformer_file_path = f"{metadata_root_path_global}{transformer_script}"
            if transformer_type == "sql":
                vw_temp_source_data_uuid = f"vw_tmp_source_data_{uuid.uuid4().hex}"
                transformed_data_df.createOrReplaceTempView(vw_temp_source_data_uuid)
                # get the sql script
                with open(tranformer_file_path, 'r') as file:
                    transformer_sql = file.read()

                transformer_sql = transformer_sql.replace("<vw_temp_source_data_uuid>", vw_temp_source_data_uuid) \
                                                    .replace("vw_temp_source_data_uuid", vw_temp_source_data_uuid) \
                                                    .replace("<bronze>", self.schema_bronze).replace("<silver>", self.schema_silver).replace("<gold>", self.schema_gold)
                if self.debug:
                    self.logger.debug(f"vw_temp_source_data_uuid: {vw_temp_source_data_uuid}")
                    self.logger.debug(f"transformer_sql: {transformer_sql}")

                # apply the transformation sql
                transformed_data_df = self.spark.sql(transformer_sql)

                # deduplicate the data
                duplicate_column = "_duplicate_row_no"
                if duplicate_column in (name.lower() for name in transformed_data_df.columns):
                    transformed_data_df = transformed_data_df.filter(f"{duplicate_column} <= 1").drop(duplicate_column)
                    self.logger.info(f"data after deduplicate: {transformed_data_df.count()}")

                if self.debug:
                    transformed_data_df.printSchema()
                    display(transformed_data_df)

            elif transformer_type == "python":
                transformer_factory = TransformerFactory(self.schema_bronze, self.schema_silver, self.schema_gold, self.spark, self.logger, self.debug)
                transformer = transformer_factory.get_transformer(transformer_name, tranformer_file_path)
                transformed_data_df = transformer.transform(transformed_data_df)
            
            else:
                err_msg = f"transformer_type: '{transformer_type}', undefined"
                self.logger.error(err_msg)
                raise Exception(err_msg)

            #self.logger.info(f"run transformer end: {transformer_name}")

            if self.debug:
                self.logger.debug(f"data after transform: {transformed_data_df.count()}")
        # end for

        if self.debug:
            self.logger.debug(f"function end: {function_name}")

        return transformed_data_df

    # function end: run


