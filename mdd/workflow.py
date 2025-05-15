#!/usr/bin/env python
# coding: utf-8

# ## workflow
# 
# New notebook

# In[1]:


# Use the 2 magic commands below to reload the modules if your module has updates during the current session. You only need to run the commands once.
# %load_ext autoreload
# %autoreload 2

# import 
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta import *
from delta.tables import *
import json
import logging

from env.mdd.metadata import *
from env.mdd.dataflow import *


# In[ ]:


class Workflow:

    def __init__(self, metadata_workflow_yml, metadata_environment_yml, spark, debug = False, metadata_log_yml = None, job_name = "job_123", job_start_timestamp = None, task_name = "task_123", task_start_timestamp = None):
        function_name = "__init__"

        logger_name = f"mdd.{self.__class__.__name__}"
        self.logger = logging.getLogger(logger_name)
        
        if debug:
            self.logger.debug(f"function begin: {function_name}")

        self.metadata_workflow_yml = metadata_workflow_yml
        self.metadata_environment_yml = metadata_environment_yml
        self.metadata_workflow = Metadata_Workflow(metadata_workflow_yml, debug)
        self.debug = debug
        self.spark = spark
        self.metadata_log_yml = metadata_log_yml
        self.job_name = job_name
        self.job_start_timestamp = job_start_timestamp
        self.task_name = task_name
        self.task_start_timestamp = task_start_timestamp

        i = metadata_workflow_yml.rfind('/')
        if i < 0:
            i = 0
        self.metadata_workflow_yml_path = metadata_workflow_yml[0:i]

        if debug:
            self.logger.debug(f"function end: {function_name}")

    def run(self):
        function_name = "run"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
        dataflows = self.metadata_workflow.dataflows
        for dataflow in dataflows:
            id = dataflow["id"]
            metadata_dataflow_yml = dataflow["metadata_yml"]
            i = metadata_dataflow_yml.rfind('/')
            if i < 0:
                metadata_dataflow_yml = f"{self.metadata_workflow_yml_path}/{metadata_dataflow_yml}"
                
            data_sync_options = dataflow["data_sync_options"]
            debug = dataflow["debug"]
            active = dataflow["active"]


            if self.debug:
                self.logger.debug(f"id: {id}")
                self.logger.debug(f"metadata_yml: {metadata_dataflow_yml}")
                self.logger.debug(f"data_sync_options: {data_sync_options}")
                self.logger.debug(f"debug: {debug}")
                self.logger.debug(f"active: {active}")

            if active:
                dataflow_helper = DataflowHelper()
                dataflow_ins = dataflow_helper.create_dataflow(metadata_dataflow_yml, self.metadata_environment_yml, data_sync_options, self.spark, debug, self.metadata_log_yml, self.job_name, self.job_start_timestamp, self.task_name, self.task_start_timestamp)
                dataflow_ins.run()

        # end for loop

        if self.debug:
                self.logger.debug(f"function end: {function_name}")
        
    # end function: run

