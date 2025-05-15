#!/usr/bin/env python
# coding: utf-8

# ## task
# 
# New notebook

# In[ ]:


# import
from env.mdd.metadata import *
from env.mdd.workflow import *
import logging


# In[ ]:


class Task:
    def __init__(self, metadata_task_yml, metadata_environment_yml, spark, metadata_log_yml = None, job_name = "job_123", job_start_timestamp = None, task_name = "task_123", task_start_timestamp = None):
        function_name = "__init__"
        self.spark = spark
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        i = metadata_task_yml.rfind('/')
        if i < 0:
            i = 0
        self.metadata_task_yml_path = metadata_task_yml[0:i]

        self.metadata_task = Metadata_Task(metadata_task_yml, False)
        self.metadata_environment_yml = metadata_environment_yml
        self.debug = self.metadata_task.debug
        self.active = self.metadata_task.active

        self.metadata_log_yml = metadata_log_yml
        self.job_name = job_name
        self.job_start_timestamp = job_start_timestamp
        self.task_name = task_name
        self.task_start_timestamp = task_start_timestamp

        if self.debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"debug: {self.debug}")
            logger.debug(f"active: {self.active}")
            logger.debug(f"function end: {function_name}")

        self.logger = logger

    
    def run(self):
        function_name = "run"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")

        if self.active:
            workflows = self.metadata_task.workflows
            
            for workflow in workflows:
                metadata_workflow_yml = workflow["workflow"]

                i = metadata_workflow_yml.rfind('/')
                if i < 0:
                    metadata_workflow_yml = f"{self.metadata_task_yml_path}/{metadata_workflow_yml}"

                debug = workflow["debug"]
                active = workflow["active"]
                if self.debug:
                    self.logger.debug(f"metadata_workflow_yml: {metadata_workflow_yml}")
                    self.logger.debug(f"debug: {debug}")
                    self.logger.debug(f"active: {active}")
                if active:
                    workflow = Workflow(metadata_workflow_yml, self.metadata_environment_yml, self.spark, debug, self.metadata_log_yml, self.job_name, self.job_start_timestamp, self.task_name, self.task_start_timestamp)
                    workflow.run()
            # end for loop
        else:
            if self.debug:
                self.logger.warning("the task is not active")

        if self.debug:
            self.logger.debug(f"function end: {function_name}")
    #end function run

