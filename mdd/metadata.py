#!/usr/bin/env python
# coding: utf-8

# ## metadata
# 
# New notebook

# In[1]:


import yaml
import json
import datetime as mdt
import pathlib
import logging

from env.mdd.common import *


# In[2]:


class Metadata:

    def __init__(self, metadata_yml, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_yml: {metadata_yml}")

        self.logger = logger
        self.debug = debug

        # get relatvie path
        i = metadata_yml.rfind('/')
        self.metadata_yml_path =  metadata_yml[0: i]
        
        # open the yaml file
        metadata_yml_file = f"{metadata_root_path_global}{metadata_yml}"
        if debug:
            logger.debug(f"metadata_yml_file: {metadata_yml_file}")
        try:    
            with open(metadata_yml_file, mode='rt') as yf:
                metadata = yaml.safe_load(yf)
                self.metadata = metadata
        except Exception as err:
            msg = f"YAML file error: {metadata_yml_file}"
            logger.error(err)
            raise Exception(msg)
        
        # get the configuraitons from the yaml file
        self.id = metadata["id"]
        self.type = metadata["type"]
        self.version = metadata["version"]
        self.author = metadata["author"]
        self.comment = metadata["comment"]

        if debug:
            logger.debug("metadata header")
            logger.debug(f"id: {self.id}")
            logger.debug(f"version: {self.version}")
            logger.debug(f"author: {self.author}")
            logger.debug(f"comment: {self.comment}")
            logger.debug(f"function end: {function_name}")
    # function end: init

    def to_json(self):
        function_name = "to_json"
        if debug:
            logger.debug(f"function begin: {function_name}")

        metadata_json = json.dumps(self.metadata)
        if debug:
            logger.debug(f"metadata_json: {metadata_json}")
            logger.debug(f"function end: {function_name}")
            
        return metadata_json
    # function end: to_json
        


# In[ ]:


class Metadata_Lander(Metadata):

    def __init__(self, metadata_yml, schema_bronze, schema_silver, schema_gold, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_yml: {metadata_yml}")
            logger.debug(f"schema_bronze: {schema_bronze}")
            logger.debug(f"schema_silver: {schema_silver}")
            logger.debug(f"schema_gold: {schema_gold}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata

        self.source_server_options = metadata["source_server_options"]
        self.data_landers = metadata["data_landers"]

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"source_server_options: {self.source_server_options}")
            logger.debug(f"data_landers: {self.data_landers}")
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[ ]:


class Metadata_Patcher(Metadata):

    def __init__(self, metadata_yml, schema_bronze, schema_silver, schema_gold, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_yml: {metadata_yml}")
            logger.debug(f"schema_bronze: {schema_bronze}")
            logger.debug(f"schema_silver: {schema_silver}")
            logger.debug(f"schema_gold: {schema_gold}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        patchers = []
        if metadata["patchers"]:
            for tbl_dict in metadata["patchers"]:
                tbl_patchers = {}
                for tbl in tbl_dict.keys():
                    tbl_value = tbl_dict[tbl]
                    tbl_key = tbl.replace("<bronze>", schema_bronze).replace("<silver>", schema_silver).replace("<gold>", schema_gold)
                    tbl_patchers[tbl_key] = tbl_value
                patchers.append(tbl_patchers)

        self.patchers = patchers

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"patchers: {self.patchers}")
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[ ]:


class Metadata_Validator(Metadata):

    def __init__(self, metadata_yml, schema_bronze, schema_silver, schema_gold, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        validators = []
        for tbl_dict in metadata["validators"]:
            tbl_validators = {}
            for tbl in tbl_dict.keys():
                tbl_value = tbl_dict[tbl]
                tbl_key = tbl.replace("<bronze>", schema_bronze).replace("<silver>", schema_silver).replace("<gold>", schema_gold)
                tbl_validators[tbl_key] = tbl_value
                if debug:
                    logger.debug(f"tbl_key: {tbl_key}")
                    logger.debug(f"tbl_value: {tbl_value}")
            validators.append(tbl_validators)

        self.validators = validators

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"validators: {self.validators}")
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[3]:


class Metadata_Environment(Metadata):

    def __init__(self, metadata_yml, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        
        # get the configuraitons from the yaml file
        # source
        self.source_lakehouse = metadata["source"]["source_lakehouse"]
        self.source_lakehouse_guid = metadata["source"]["source_lakehouse_guid"]
        self.source_data_root_path = metadata["source"]["source_data_root_path"]
        # destination
        self.destination_lakehouse = metadata["destination"]["destination_lakehouse"]
        self.destination_lakehouse_guid = metadata["destination"]["destination_lakehouse_guid"]
        self.destination_bronze_schema = metadata["destination"]["destination_bronze_schema"]
        self.destination_silver_schema = metadata["destination"]["destination_silver_schema"]
        self.destination_gold_schema = metadata["destination"]["destination_gold_schema"]

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"source_lakehouse: {self.source_lakehouse}")
            logger.debug(f"source_lakehouse_guid: {self.source_lakehouse_guid}")
            logger.debug(f"source_data_root_path: {self.source_data_root_path}")
            logger.debug(f"destination_lakehouse: {self.destination_lakehouse}")
            logger.debug(f"destination_lakehouse_guid: {self.destination_lakehouse_guid}")
            logger.debug(f"destination_bronze_schema: {self.destination_bronze_schema}")
            logger.debug(f"destination_silver_schema: {self.destination_silver_schema}")
            logger.debug(f"destination_gold_schema: {self.destination_gold_schema}")
            logger.debug(f"function end: {function_name}")
    # function end: init
        


# In[ ]:


class Metadata_Dataflow(Metadata):
    def __init__(self, metadata_yml, logger, debug = False):
        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        self.dataflow_type = metadata["dataflow_type"]


# In[4]:


class Metadata_Onboarding_Dataflow(Metadata):

    def __init__(self, metadata_yml, logger, debug = False):
        function_name = "__init__"
        
        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        
        # get the configuraitons from the yaml file
        self.dataflow_type = metadata["dataflow_type"]

        # reader
        self.source_data_format = metadata["reader"]["source_data_format"]
        self.source_data_relative_path = metadata["reader"]["source_data_relative_path"]
        self.source_file_name_regex = metadata["reader"]["source_file_name_regex"]
        self.Source_data_read_options = metadata["reader"]["Source_data_read_options"]
        self.source_data_schema = metadata["reader"]["source_data_schema"]

        # writer
        self.destination_table_schema = None
        if "destination_table_schema" in metadata["writer"]:
            self.destination_table_schema = metadata["writer"]["destination_table_schema"]
        self.destination_table_name = metadata["writer"]["destination_table_name"]
        self.destination_projected_sql = metadata["writer"]["destination_projected_sql"]
        self.destination_write_mode = metadata["writer"]["destination_write_mode"]
        self.destination_write_options = metadata["writer"]["destination_write_options"]

        # validators
        self.validators = metadata["validators"]
        # set default path to /validate/
        if self.validators is not None and self.validators != "":
            i = self.validators.rfind('/')
            if i < 0:
                self.validators = f"{self.metadata_yml_path}/validate/{self.validators}"

        self.pre_onboard_script = None
        if "pre_onboard_script" in metadata:
            self.pre_onboard_script = metadata["pre_onboard_script"]

        self.post_onboard_script = None
        if "pre_onboard_script" in metadata:
            self.post_onboard_script = metadata["post_onboard_script"]

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"source_data_format: {self.source_data_format}")
            logger.debug(f"source_data_relative_path: {self.source_data_relative_path}")
            logger.debug(f"source_file_name_regex: {self.source_file_name_regex}")
            logger.debug(f"Source_data_read_options: {self.Source_data_read_options}")
            logger.debug(f"source_data_schema: {self.source_data_schema}")
            logger.debug(f"destination_table_name: {self.destination_table_name}")
            logger.debug(f"destination_projected_sql: {self.destination_projected_sql}")
            logger.debug(f"destination_write_mode: {self.destination_write_mode}")
            logger.debug(f"destination_write_options: {self.destination_write_options}")
            logger.debug(f"validators: {self.validators}")
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[ ]:


class Metadata_Transform_Dataflow(Metadata):

    def __init__(self, metadata_yml, logger, debug = False):
        function_name = "__init__"
        
        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        
        # get the configuraitons from the yaml file
        self.dataflow_type = metadata["dataflow_type"]
        
        # reader
        self.source_table_schema = metadata["reader"]["source_table_schema"]
        self.source_table_name = metadata["reader"]["source_table_name"]

        # writer
        self.destination_table_schema = metadata["writer"]["destination_table_schema"]
        self.destination_table_name = metadata["writer"]["destination_table_name"]
        self.destination_projected_sql = metadata["writer"]["destination_projected_sql"]
        self.destination_write_mode = metadata["writer"]["destination_write_mode"]
        self.destination_write_options = metadata["writer"]["destination_write_options"]

        # transformers
        self.transformers = metadata["transformers"]
        # set default path to /transform/
        for transformer in self.transformers:
            transformer_script = transformer["transformer_script"]
            i = transformer_script.rfind('/')
            if i < 0:
                transformer["transformer_script"] = f"{self.metadata_yml_path}/transform/{transformer_script}"

        # validators
        self.validators = metadata["validators"]
        # set default path to /validate/
        if self.validators is not None and self.validators != "":
            i = self.validators.rfind('/')
            if i < 0:
                self.validators = f"{self.metadata_yml_path}/validate/{self.validators}"

        # patchers
        self.patchers = metadata["patchers"]
        # set default path to /validate/
        if self.patchers is not None and self.patchers != "":
            i = self.patchers.rfind('/')
            if i < 0:
                self.patchers = f"{self.metadata_yml_path}/patch/{self.patchers }"

        self.pre_transform_script = metadata["pre_transform_script"]
        self.post_transform_script = metadata["post_transform_script"]

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"source_table_schema: {self.source_table_schema}")
            logger.debug(f"source_table_name: {self.source_table_name}")
            logger.debug(f"destination_table_schema: {self.destination_table_schema}")
            logger.debug(f"destination_table_name: {self.destination_table_name}")
            logger.debug(f"destination_projected_sql: {self.destination_projected_sql}")
            logger.debug(f"destination_write_mode: {self.destination_write_mode}")
            logger.debug(f"destination_write_options: {self.destination_write_options}")
            logger.debug(f"transformers: {self.transformers}")
            logger.debug(f"validators: {self.validators}")
            logger.debug(f"patchers: {self.patchers}")
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[5]:


class Metadata_Workflow(Metadata):

    def __init__(self, metadata_yml, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        
        # get the configuraitons from the yaml file
        self.active = metadata["active"]
        self.dataflows = metadata["dataflows"]

        if debug:
            logger.debug("metadata detail:")
            logger.debug(f"active: {self.active}")
            logger.debug(f"dataflows: {self.dataflows}")
            logger.debug(f"function end: {function_name}")

    # function end: init


# In[6]:


class Metadata_Task(Metadata):

    def __init__(self, metadata_yml, logger, debug = False):
        function_name = "__init__"
        if debug:
            logger.debug("##debug begin: class Metadata_Task.init")

        super().__init__(metadata_yml, logger, debug)

        metadata = self.metadata
        
        # get the configuraitons from the yaml file
        # reader
        #self.environment = metadata
        self.debug = metadata["debug"]
        self.active = metadata["active"]
        self.workflows = metadata["workflows"]

        if debug:
            logger.debug("metadata detail:")
            #logger.debug(f"environment: {self.environment}")
            logger.debug(f"debug: {self.debug}")
            logger.debug(f"active: {self.active}")
            logger.debug(f"workflows: {self.workflows}")
            logger.debug("##debug end: class Metadata_task.init\n")
    # function end: init


# In[ ]:


class Metadata_Log(Metadata):

    def __init__(self, metadata_yml, log_folder, log_timestamp, log_file_name):
        function_name = "__init__"

        super().__init__(metadata_yml, None, False)
        self.log_folder = log_folder
        self.log_timestamp = log_timestamp
        self.log_file_name = log_file_name
        self.log_year = log_timestamp[0:4]
        self.log_month = log_timestamp[5:7]
        self.log_day = log_timestamp[8:10]

    def get_config(self):
        config = self.metadata

        handlers = config["handlers"]
        for x in handlers:
            handler = handlers[x]
            key = "filename"
            if key in handler:
                value = handler[key].replace("<log_folder>", self.log_folder) \
                        .replace("<log_year>", self.log_year) \
                        .replace("<log_month>", self.log_month) \
                        .replace("<log_day>", self.log_day) \
                        .replace("<log_timestamp>", self.log_timestamp) \
                        .replace("<file_name>", self.log_file_name) 
            
                handler[key] = value
                

                # create the folder if not exists
                pathlib.Path(value).parent.mkdir(parents = True, exist_ok = True)
        
        return config

    # function end: init


# In[ ]:


class Metadata_Model(Metadata):
    def __init__(self, metadata_yml, debug = False):

        super().__init__(metadata_yml, None, debug)

        metadata = self.metadata
        self.models = metadata["models"]

