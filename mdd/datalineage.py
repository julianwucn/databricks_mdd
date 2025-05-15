#!/usr/bin/env python
# coding: utf-8

# ## datalineage
# 
# New notebook

# In[3]:


from env.mdd.utilhelper import *
import graphviz


# In[4]:


class DataLineage:
    def __init__(self, spark, logger, debug):
        self.spark = spark
        self.logger = logger
        self.debug = debug

    def report(self, tables):
        dependencies = dict.fromkeys(tables, {})

        for table in tables:
            tableutil = TableUtil(table, self.spark, self.debug)
            table_property = tableutil.get_table_properties()

            key = "mdd.source.primary"
            sources_primary = []
            if key in table_property:
                sources = [x.strip() for x in table_property[key].split(",")]
                sources = list(dict.fromkeys(sources))
                sources_primary = sources

            key = "mdd.source.secondary"
            sources_secondary = []
            if key in table_property:
                sources = [x.strip() for x in table_property[key].split(",")]
                sources = list(dict.fromkeys(sources))
                sources_secondary = sources
                
            key = "mdd.patcher.source"
            sources_patcher = []
            if key in table_property:
                sources = [x.strip() for x in table_property[key].split(",")]
                sources = list(dict.fromkeys(sources))
                sources_patcher = sources

            source_dict = {}
            source_dict["primary"] = sources_primary
            source_dict["secondary"] = list(set(sources_secondary) - set(sources_primary))
            source_dict["patcher"] = list(set(sources_patcher) - set(sources_secondary) - set(sources_primary))
            
            dependencies[table] = source_dict
        
        return dependencies
    
    def report_dot(self, tables):
        dependencies = self.report(tables)

        dot = graphviz.Digraph('data_lineage') 

        for table in dependencies:
            dot.node(table, shape = "rectangle", color = "black")
            for dt in dependencies[table]["primary"]:
                dot.edge(dt, table, color = "black", tooltip = "primary")
            for dt in dependencies[table]["secondary"]:
                dot.edge(dt, table, color = "blue", tooltip = "secondary")
            for dt in dependencies[table]["patcher"]:
                dot.edge(dt, table, color = "green", tooltip = "patcher")

        dot.graph_attr = {"rankdir": "LR"}

        return dot

