#!/usr/bin/env python
# coding: utf-8

# ## datareaderfactory
# 
# New notebook

# In[ ]:


# import
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable



# In[ ]:


class DataReaderFactory:
    registry = {}

    @classmethod
    def register(cls, name: str) -> Callable:
        def inner_wrapper(wrapped_class: DataReaderBase) -> Callable:
            cls.registry[name] = wrapped_class
            return wrapped_class

        return inner_wrapper

    @classmethod
    def create_datareader(cls, name:str, source_options, spark, debug) -> DataReaderBase:
        if name not in cls.registry:
            return None
        
        datareader_class = cls.registry[name] 
        datareader = datareader_class(source_options, spark, debug)

        return datareader


# In[ ]:


class DataReaderBase(ABC):
    def __init__(self, source_options, spark, debug = False):
        self.source_options = source_options
        self.spark = spark
        self.debug = debug

    @abstractmethod
    def read(self, source_path) -> dataframe:
        pass

