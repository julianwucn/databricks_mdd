#!/usr/bin/env python
# coding: utf-8

# ## datalander
# 
# New notebook

# In[ ]:


import env.mdd.common as common
from env.mdd.metadata import *
from env.mdd.datalanderfactory import *

from inspect import isclass
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module

# import all data landers
package_dir = common.datalanders_package_path
module_path_list = package_dir.split("/")
module_path_list.remove(".")
module_path = ".".join(module_path_list)
for (_, module_name, _) in iter_modules([package_dir]):
    module_name = f"{module_path}.{module_name}"
    module = import_module(module_name) 


# In[ ]:


class DataLander:
    def __init__(self, metadata_environment_yml, metadata_lander_yml, spark, logger, debug = False):
        self.metadata_environment_yml = metadata_environment_yml
        self.metadata_lander_yml = metadata_lander_yml
        self.spark = spark
        self.logger = logger
        self.debug = debug

    def run(self):
        metadata_environment = Metadata_Environment(self.metadata_environment_yml, self.logger, True)
        source_data_root_path = metadata_environment.source_data_root_path
        destination_lakehouse = metadata_environment.destination_lakehouse
        destination_lakehouse_guid = metadata_environment.destination_lakehouse_guid
        destination_bronze_schema = metadata_environment.destination_bronze_schema
        destination_silver_schema = metadata_environment.destination_silver_schema
        destination_gold_schema = metadata_environment.destination_gold_schema


        metadata_lander = Metadata_Lander(self.metadata_lander_yml, destination_bronze_schema, destination_bronze_schema, destination_gold_schema, self.logger, True)
        source_server_options = metadata_lander.source_server_options
        server_type = source_server_options["server_type"]

        datalander = DataLanderFactory.create_datalander(server_type, self.metadata_environment_yml, self.metadata_lander_yml, self.spark, self.logger, self.debug)
        if datalander is None:
            err_msg = f"the data lander for '{server_type}' is not registered"
            self.logger.error(err_msg)
            raise Exception(err_msg)

        datalander.run()


