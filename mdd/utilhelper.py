#!/usr/bin/env python
# coding: utf-8

# ## utilhelper
# 
# New notebook

# In[1]:


# import
from pyspark.sql.functions import *
import notebookutils 
import datetime as mdt
import re
import json

import logging


# In[ ]:


class TableUtil:

    def __init__(self, table_name, spark, debug = False):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        self.table_name = table_name
        self.spark = spark
        self.logger = logger
        self.debug = debug
        
        if debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"table_name: {table_name}")

        
        if not spark.catalog.tableExists(table_name):
            error_msg = f"'{table_name}' does not exist"
            self.logger.error(error_msg)
            raise Exception(error_msg)

        if debug:
            self.logger.debug(f"function end: {function_name}")
    # function end: init

    def clean_table(self):
        sql_statement = f"delete from {self.table_name};"
        self.spark.sql(sql_statement)
        sql_statement = f"alter table {self.table_name} unset TBLPROPERTIES('mdd.source.primary', 'mdd.source.secondary', 'mdd.patcher.source', 'mdd.patcher.timestamp', 'mdd.validator.expectation', 'mdd.validator.timestamp');"
        self.spark.sql(sql_statement)
        sql_statement = f"delete from mdd.etl_job_files where table_name = '{self.table_name}';"
        self.spark.sql(sql_statement)


    def get_table_property_dict_value(self, property_name, property_key, is_timestamp = False):
        table_property_dict = self.get_table_properties()
        if self.debug:
            self.logger.debug(f"table_property_dict: {table_property_dict}")
            self.logger.debug(f"property_key: {property_key}")

        property_dict = {}
        if property_name in table_property_dict:
            property_str = table_property_dict[property_name]
            property_dict = json.loads(property_str)
        
        if self.debug:
            self.logger.debug(f"property_dict: {property_dict}")
        
        property_value = None
        if property_key in property_dict:
            property_value_str = property_dict[property_key]
            if self.debug:
                self.logger.debug(f"property_value_str: {property_value_str}")
            
            if is_timestamp:
                property_value = mdt.datetime.strptime(property_value_str, "%Y-%m-%d, %H:%M:%S.%f") 
            else:
                property_value = property_value_str

        if self.debug:
            self.logger.debug(f"property_value: {property_value}")

        return property_value

    def set_table_property_list(self, property_name, property_values):
        table_property_dict = self.get_table_properties()
        if self.debug:
            self.logger.debug(f"table_property_dict: {table_property_dict}")
            self.logger.debug(f"property_values: {property_values}")

        property_list = []
        if property_name in table_property_dict:
            property_str = table_property_dict[property_name]
            property_list = [column.strip() for column in property_str.split(",")]
        if self.debug:
            self.logger.debug(f"property_list: {property_list}")
        
        if property_values is not None:
            if type(property_values) == str:
                property_values_list = [column.strip() for column in property_values.split(",")]
            else:
                property_values_list = property_values

            final_list = list(set(property_list) | set(property_values_list))
            final_list.sort()
            property_str = ", ".join([str(t) for t in final_list])

            property_set_sql = f"alter table {self.table_name} set TBLPROPERTIES ({property_name} = '{property_str}')"
            if self.debug:
                self.logger.debug(f"final_list: {final_list}")
                self.logger.debug(f"property_str: {property_str}")
                self.logger.debug(f"property_set_sql: {property_set_sql}")

            self.spark.sql(property_set_sql)

    def set_table_property_dict(self, property_name, property_key, property_value):
        table_property_dict = self.get_table_properties()
        if self.debug:
            self.logger.debug(f"table_property_dict: {table_property_dict}")
            self.logger.debug(f"property_key: {property_key}")
            self.logger.debug(f"property_value: {property_value}")

        property_dict = {}
        if property_name in table_property_dict:
            property_str = table_property_dict[property_name]
            property_dict = json.loads(property_str)
            
        if self.debug:
            self.logger.debug(f"property_dict: {property_dict}")
        
        property_dict[property_key] = property_value
        property_str = json.dumps(property_dict)
        property_set_sql = f"alter table {self.table_name} set TBLPROPERTIES ({property_name} = '{property_str}')"
        if self.debug:
            self.logger.debug(f"property_dict: {property_dict}")
            self.logger.debug(f"property_str: {property_str}")
            self.logger.debug(f"property_set_sql: {property_set_sql}")

        self.spark.sql(property_set_sql)

    def get_table_properties(self):
        function_name = "get_table_properties"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        table_property_df = self.spark.sql(f"SHOW TBLPROPERTIES {self.table_name}")
        table_property_dict = {row["key"]: row["value"] for row in table_property_df.collect()}

        # parse primary key to list
        table_property_pk = "mdd.primaryKey"
        if (table_property_pk in table_property_dict.keys()):
            table_primarykey = table_property_dict[table_property_pk]
            if table_primarykey is not None:
                table_primarykey_columns = [column.strip() for column in table_primarykey.split(",")]
                
        table_property_dict[table_property_pk] = table_primarykey_columns
        
        if self.debug:
            self.logger.debug(f"table_property_dict: {table_property_dict}")
            self.logger.debug(f"function end: {function_name}")
        
        return table_property_dict
    
    def get_table_merge_info(self, timestamp_from, timestamp_to):
        table_history_df = self.spark.sql(f"describe history {self.table_name}")
        table_history_df = table_history_df.filter((table_history_df.operation == "MERGE") & (table_history_df.timestamp > timestamp_from ) & (table_history_df.timestamp < timestamp_to)) 
        table_history_df = table_history_df.sort(table_history_df.timestamp.desc()).select("operationMetrics")

        merge_dict = {}
        if not table_history_df.isEmpty():
            merge_dict = table_history_df.first()[0]

        if self.debug:
            self.logger.debug(f"merge_dict: {merge_dict}")
        
        return merge_dict

    def get_table_history(self):
        function_name = "get_table_history"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        table_history_df = self.spark.sql(f"describe history {self.table_name}")
        table_history_version_min = 0
        table_history_version_max = 0
        table_history_version_agg_df = table_history_df.agg(min(table_history_df.version).alias("version_min"), max(table_history_df.version).alias("version_min"))
        if not table_history_version_agg_df.isEmpty():
            table_history_version_min = table_history_version_agg_df.first()[0]
            table_history_version_max = table_history_version_agg_df.first()[1]
        
        table_history_version_cdf = 0
        table_property_dict = self.get_table_properties()
        if table_property_dict["delta.enableChangeDataFeed"] == "true":   
            table_history_properties_df = table_history_df.select(table_history_df.version, explode(table_history_df.operationParameters)).filter("key = 'properties'")
            table_history_cdf_df = table_history_properties_df.filter(table_history_properties_df.value.contains("\"delta.enableChangeDataFeed\":\"true\"")) \
                                                    .select(max(table_history_properties_df.version))
            if not table_history_cdf_df.isEmpty():
                table_history_version_cdf = table_history_cdf_df.first()[0]
        
        # get the max timestamp of the records
        table_timestamp_df = self.spark.sql(f"select max(_record_timestamp) from {self.table_name}")
        table_history_timestamp_max = None
        if not table_timestamp_df.isEmpty():
            table_history_timestamp_max = table_timestamp_df.first()[0]

        table_history_dict = {
                                "table_history_version_min": table_history_version_min, 
                                "table_history_version_max": table_history_version_max,
                                "table_history_version_cdf": table_history_version_cdf,
                                "table_history_timestamp_max": table_history_timestamp_max
                            }

        if self.debug:
            self.logger.debug(f"table_history_dict: {table_history_dict}")
            self.logger.debug(f"function end: {function_name}")

        return table_history_dict


# In[ ]:


class FileUtil:

    def __init__(self, debug = False):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        self.logger = logger
        self.debug = debug

        if debug:
            self.logger.debug(f"function begin: {function_name}")

        if debug:
            self.logger.debug(f"function end: {function_name}")
    # function end: init

    # get all files in the root path and the sub folders 
    # file path or file name must include date with YYYMMDD format
    def get_files_recursive(self, file_path, source_files):
        list_files = notebookutils.fs.ls(file_path)
        for file_info in list_files: 
            if file_info.isDir:
                # if it is a folder, get the files
                self.get_files_recursive(file_info.path, source_files)
            elif file_info.isFile:
                source_files.append({"file_name": file_info.name, 
                                    "file_path": file_info.path, 
                                    "file_modification_timestamp": mdt.datetime.fromtimestamp(file_info.modifyTime / 1000), 
                                    "file_size": file_info.size})
    # function end: get_files_recursive

    def get_files(self, file_path, file_name_regex):
        function_name = "get_files"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"file_path: {file_path}")
            self.logger.debug(f"file_name_regex: {file_name_regex}")

        
        source_files_regex = []
        file_path_list = file_path.split(",")
        for file_path_i in file_path_list:
            source_files = []
            file_path_i_trimmed = file_path_i.strip()
            self.get_files_recursive(file_path_i_trimmed, source_files)
            if self.debug:
                self.logger.debug(f"source_files: {len(source_files)}")
                #self.logger.debug(f"source_files: {source_files}\n")

        
            if not (file_name_regex is None or file_name_regex == ""):
                for file_item in source_files[:]:  
                    # only get the files which match the regex 
                    if re.search(file_name_regex, file_item["file_name"]):
                        # get relative file path
                        file_item_path = re.search(f"{file_path_i_trimmed}.*", file_item["file_path"]).group()
                        # extract YYYYMMDD from file path from left to right
                        file_item_date_match = re.search(r"\d{4}\d{2}\d{2}", file_item_path)
                        if file_item_date_match:
                            file_item_date = file_item_date_match.group()
                            file_item_date = f"{file_item_date[0:4]}-{file_item_date[4:6]}-{file_item_date[6:8]}"
                        else:
                            file_item_date = "1900-01-01" 
                        source_files_regex.append({"file_name": file_item["file_name"],
                                                    "file_path": file_item_path,
                                                    "file_modification_timestamp": file_item["file_modification_timestamp"],
                                                    "file_size": file_item["file_size"],
                                                    "file_date": file_item_date
                                                    }
                                                )

        if self.debug:
            self.logger.debug(f"source_files_regex: {len(source_files_regex)}")
            self.logger.debug(f"source_files_regex: {source_files_regex}\n")
            self.logger.debug(f"function end: {function_name}")
        
        return source_files_regex
    # function end: get_files

    def posi_file_rename(self, file_path, file_name_regex):
        # rename the posi files
        #file_path = "Files/pocdata/live/posi/"
        #file_name_regex_list = ["^(ADJUSTS)(\.DBF)$", "^(CHKHDR)(\.DBF)$", "^(CHKITEMS)(\.DBF)$", "^(FCOSTN)(\.DBF)$", "^(JOURNAL)(\.DBF)$"]
        #for file_name_regex in file_name_regex_list:
        fileutil = FileUtil(False)
        source_files = fileutil.get_files(file_path, file_name_regex)

        for file_dict in source_files:
            file_path_old = file_dict['file_path']
            
            # extract YYYYMMDD from path
            file_date_str = re.search(r"/\d{2}-\d{2}-\d{2}/", file_path_old).group()
            file_date_list = file_date_str.strip("/").split("-")
            file_date = f"20{file_date_list[2]}{file_date_list[0]}{file_date_list[1]}"
            #self.logger.debug(f"file_date: {file_date}")
            file_store_str = re.search(r"/\d{4}/", file_path_old).group()
            file_store =  file_store_str.strip("/")
            #self.logger.debug(f"file_store: {file_store}")
            file_name_new =  f"{file_date}_{file_store}_{file_dict['file_name']}"
            file_path_new = re.sub(file_name_regex.strip("^"), file_name_new, file_path_old)

            file_dict['file_path'] = file_path_new

            if self.debug:
                self.logger.debug(f"file_rename: {file_path_old} -> {file_path_new}")
            notebookutils.fs.mv(file_path_old, file_path_new)


