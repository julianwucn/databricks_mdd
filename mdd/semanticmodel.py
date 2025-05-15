#!/usr/bin/env python
# coding: utf-8

# ## semanticmodel
# 
# New notebook

# In[1]:


import sempy.fabric as fabric
import logging

from env.mdd.metadata import *


# In[5]:


class SemanticModelManager:
    def __init__(self, metadata_model_yml,  debug):
        self.debug = debug

        metadata = Metadata_Model(metadata_model_yml, debug)
        self.models = metadata.models
        if debug:
            self.logger.debug(f"models: {self.models }")

        logger_name = f"mdd.{self.__class__.__name__}"
        self.logger = logging.getLogger(logger_name)
        if debug:
            self.logger.debug(f"logger_name: {logger_name}")
            

    def refresh(self):
        # models: {"semantic_model": "refresh_type"} 
        # refresh_type: full, clearValues, calculate, dataOnly, automatic, and defragment   

        for model in self.models:
            semantic_model = model["model_name"]
            refresh_type = model["refresh_type"]
            self.logger.info(f"reframe semantic model: {semantic_model} -> {refresh_type}")
            refresh_request_id = fabric.refresh_dataset(dataset = semantic_model, refresh_type = refresh_type)

            if self.debug:
                self.logger.debug(f"refresh_request_id: {refresh_request_id}")

