#!/usr/bin/env python
# coding: utf-8

# ## patcher
# 
# New notebook

# In[1]:


from abc import abstractmethod, ABC
from inspect import getmembers, isclass, isabstract
import sys
import importlib.util
import re
import os
import uuid
from delta import *
from delta.tables import *
from pyspark.sql.functions import *
import datetime as mdt
import logging

from env.mdd.common import *
from env.mdd.metadata import *
from env.mdd.utilhelper import *


# In[ ]:


class PatcherAbs(ABC):
    def __init__(self, schema_bronze, schema_silver, schema_gold, spark, logger, debug):
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold
        
        self.spark = spark
        self.logger = logger
        self.debug = debug

    @abstractmethod
    def patch(self):
        pass


# In[ ]:


class PatcherFactory:

    def __init__(self, schema_bronze, schema_silver, schema_gold, spark, logger, debug):
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold

        self.spark = spark
        self.logger = logger
        self.debug = debug

    def create_patcher(self, patcher_name, patcher_path):
        function_name = "create_patcher"
        module_file = patcher_path
        module_name = os.path.basename(module_file).replace(".py", "")

        if self.debug:
            self.logger.debug(f"patcher_name: {patcher_name}")
            self.logger.debug(f"module_file: {module_file}")
            self.logger.debug(f"module_name: {module_name}")

        module_spec = importlib.util.spec_from_file_location(module_name, module_file)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)

        patcher = None
        classes = getmembers(module)
        if self.debug:
            self.logger.debug(f"classes: {classes}")
        for name, _type in classes:
            if self.debug:
                self.logger.debug(f"name: {name}")
                self.logger.debug(f"_type: {_type}")
            if isclass(_type) and not isabstract(_type) and issubclass(_type, PatcherAbs) and name == patcher_name:
                if self.debug:
                    self.logger.debug(f"patcher_name: {patcher_name} is found")

                patcher = _type(self.schema_bronze, self.schema_silver, self.schema_gold, self.spark, self.logger, self.debug)
                break

        if patcher is None:
            self.logger.error(f"The patcher class '{patcher_name}' is not found in '{patcher_path}'")

        return patcher



                


# In[ ]:


class PatcherHelper:

    def __init__(self, destination_table_full_path, table_schema_name, metadata_patcher_yml, schema_bronze, schema_silver, schema_gold, spark, debug):
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        self.destination_table_full_path = destination_table_full_path
        self.table_schema_name = table_schema_name
        
        metadata = Metadata_Patcher(metadata_patcher_yml, schema_bronze, schema_silver, schema_gold, logger, debug)
        self.patchers = metadata.patchers

        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold
        self.spark = spark
        self.logger = logger
        self.debug = debug

        if debug:
            self.logger.debug(f"patchers: {self.patchers}")

    def run(self):
        function_name = "run"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        source_alias = "source"
        target_alias = "target"
        destination_data_dt = DeltaTable.forPath(self.spark, self.destination_table_full_path)
        tableutil = TableUtil(self.table_schema_name, self.spark, self.debug)

        for tbl_list in self.patchers:
            for tbl in tbl_list.keys():
                if tbl == self.table_schema_name:
                    patchers = tbl_list[tbl]
                    for patcher in patchers:
                        patcher_name = patcher["patcher"]
                        patcher_type = patcher["patcher_type"]
                        patcher_script = patcher["patcher_script"]
                        patcher_join_columns = patcher["patcher_join_columns"]
                        patcher_source_tables = patcher["patcher_source_tables"]
                        patcher_active = patcher["active"]
                        patcher_timestamp = mdt.datetime.now()
                        
                        if patcher_active:
                            
                             # must set the patcher_source_tables, this will be used for tracking table dependencies
                            if patcher_source_tables is None or patcher_source_tables == "":
                                self.logger.error(f"patcher_source_tables of {patcher_name}: undefined")
                            if patcher_name is None or patcher_name == "":
                                self.logger.error(f"patcher_name of {patcher_name}: undefined")

                            if patcher_type == "sql":
                                patch_data_sql = patcher_script.replace("<bronze>", self.schema_bronze).replace("<silver>", self.schema_silver).replace("<gold>", self.schema_gold)
                                patch_data_df = self.spark.sql(patch_data_sql)

                            elif patcher_active and patcher_type == "notebook":
                                patcher_factory = PatcherFactory(self.schema_bronze, self.schema_silver, self.schema_gold, self.spark, self.logger, self.debug)
                                patcher_path = f"{metadata_root_path_global}{patcher_script}"
                                patcher = patcher_factory.create_patcher(patcher_name, patcher_path)
                                patch_data_df = patcher.patch()

                            else:
                                self.logger.error(f"patcher_type '{patcher_type}' of '{patcher_name}' is undefined for table '{self.table_schema_name}'")

                            # get the last source timestamp
                            property_name_patcher_timestamp = "mdd.patcher.timestamp"
                            patcher_last_timestmap = tableutil.get_table_property_dict_value(property_name_patcher_timestamp, patcher_name, True)
                            if self.debug:
                                self.logger.debug(f"patcher_last_timestmap: {patcher_last_timestmap}")

                            # only get the new data
                            record_timestamp_column = "_record_timestamp"
                            if record_timestamp_column not in patch_data_df.schema.names:
                                msg = f"'{record_timestamp_column}' must be included in 'patcher_script' of {patcher_name}"
                                self.logger.error(msg)

                            if patcher_last_timestmap is not None:
                                patch_data_df = patch_data_df.filter(patch_data_df[record_timestamp_column] > patcher_last_timestmap)

                            patch_data_df = patch_data_df.drop(record_timestamp_column) \
                                            .withColumn(record_timestamp_column,lit(current_timestamp()))

                            if patch_data_df.isEmpty():
                                self.logger.info(f"patch: {patcher_name}, no new data, skipped")
                            else:
                                self.logger.info(f"patch: {patcher_name}")
                                if self.debug:
                                    self.logger.debug(f"patch_data_df: {patch_data_df.count()}")
                                    patch_data_df.printSchema()

                                # update the desintation table
                                columns_join = [column.strip() for column in patcher_join_columns.split(",")]
                                merge_join_condition = " AND ".join([f"{target_alias}.{col} = {source_alias}.{col}" for col in columns_join])
                                columns_all = patch_data_df.schema.names
                                columns_to_update = [column for column in columns_all if column not in columns_join]
                                columns_to_update_condition = [column for column in columns_all if column not in columns_join and column not in [record_timestamp_column]]
                                merge_update_condition = " OR ".join([f"({target_alias}.{col} <> {source_alias}.{col} OR {target_alias}.{col} IS NULL AND {source_alias}.{col} IS NOT NULL OR {target_alias}.{col} IS NOT NULL AND {source_alias}.{col} IS NULL)" for col in columns_to_update_condition])
                                merge_update_set = dict([(f"{target_alias}.{key}", f"{source_alias}.{val}") for key, val in zip(columns_to_update[::1], columns_to_update[::1])])

                                if self.debug:
                                    self.logger.debug(f"columns_join: {columns_join}")
                                    self.logger.debug(f"columns_all: {columns_all}")
                                    self.logger.debug(f"columns_to_update: {columns_to_update}")
                                    self.logger.debug(f"merge_join_condition: {merge_join_condition}")
                                    self.logger.debug(f"merge_update_condition: {merge_update_condition}")
                                    self.logger.debug(f"merge_update_set: {merge_update_set}")

                                merge_timestamp_from = mdt.datetime.now()
                                (
                                    destination_data_dt.alias(target_alias)
                                                        .merge(source = patch_data_df.alias(source_alias), condition = merge_join_condition)
                                                        .whenMatchedUpdate(condition = merge_update_condition, set = merge_update_set)
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
                                    #numTargetRowsInserted = merge_info_dict["numTargetRowsInserted"]
                                    numTargetRowsUpdated = merge_info_dict["numTargetRowsUpdated"]
                                    #numTargetRowsDeleted = merge_info_dict["numTargetRowsDeleted"]
                                    self.logger.info(f"total source rows: {numSourceRows}")
                                    #self.logger.info(f"total rows inserted: {numTargetRowsInserted}")
                                    self.logger.info(f"total rows updated: {numTargetRowsUpdated}")
                                    #self.logger.info(f"total rows deleted: {numTargetRowsDeleted}")

                                # record the source name and patcher timestampmerge_info_dict
                                property_name = "mdd.patcher.timestamp"
                                patcher_timestamp_str = patcher_timestamp.strftime("%Y-%m-%d, %H:%M:%S.%f")
                                tableutil.set_table_property_dict(property_name, patcher_name, patcher_timestamp_str)

                                property_name = "mdd.patcher.source"
                                patcher_source_tables = patcher_source_tables.replace("<bronze>", self.schema_bronze).replace("<silver>", self.schema_silver).replace("<gold>", self.schema_gold)
                                tableutil.set_table_property_list(property_name, patcher_source_tables)
                            
                    # end for 
                # end if
            # end for tbl
        # end for tbl_list

        if self.debug:
            self.logger.debug(f"function end: {function_name}")
    # function end: run


