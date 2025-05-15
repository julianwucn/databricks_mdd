#!/usr/bin/env python
# coding: utf-8

# ## datalanderfactory
# 
# New notebook

# In[ ]:


# import
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable



# In[ ]:


class DataLanderFactory:
    registry = {}

    @classmethod
    def register(cls, name: str) -> Callable:
        def inner_wrapper(wrapped_class: DataLanderBase) -> Callable:
            cls.registry[name] = wrapped_class
            return wrapped_class

        return inner_wrapper

    @classmethod
    def create_datalander(cls, name:str, metadata_environment_yml, metadata_lander_yml, spark, logger, debug) -> DataLanderBase:
        if name not in cls.registry:
            return None
        
        datalander_class = cls.registry[name] 
        datalander = datalander_class(metadata_environment_yml, metadata_lander_yml, spark, logger, debug)

        return datalander


# In[ ]:


class DataLanderBase(ABC):
    def __init__(self, metadata_environment_yml, metadata_lander_yml, spark, logger,  debug = False):
        self.metadata_environment_yml = metadata_environment_yml
        self.metadata_lander_yml = metadata_lander_yml
        self.spark = spark
        self.logger = logger
        self.debug = debug

    @abstractmethod
    def run(self):
        pass

