#!/usr/bin/env python
# coding: utf-8

# ## dataquality
# 
# New notebook

# In[3]:


import json
from env.mdd.utilhelper import *


# In[4]:


class DataQuality:
    def __init__(self, spark, logger, debug):
        self.spark = spark
        self.logger = logger
        self.debug = debug

    def report(self, tables):
        spark = self.spark
        debug = self.debug
        
        property_name = "mdd.validator.expectation"

        dataquality = {}
        for table in tables:
            dataquality[table] = {}

            # get row validation result
            sql_query = f"""select _data_validation_expectations, count(*) as total_count 
                        from {table} 
                        where _data_validation_expectations <> ''
                            and _deleted_by_source = 0 
                            and _deleted_by_validation = 0
                        group by _data_validation_expectations"""
            df = spark.sql(sql_query).withColumn("_data_validation_expectations", split(col("_data_validation_expectations"), ","))
            df = df.select(explode(df._data_validation_expectations).alias("violated_expectation"), df.total_count)
            pandas_df = df.groupBy("violated_expectation").agg(sum("total_count").alias("violated_rows")).toPandas()
            violation_dict = dict(zip(pandas_df["violated_expectation"], pandas_df["violated_rows"]))

            # get total rows
            total_rows = 0
            sql_query = f"""select count(*) as total_rows 
                        from {table}
                        where _deleted_by_source = 0"""
            df = spark.sql(sql_query)
            if not df.isEmpty():
                total_rows = df.first()[0]
            dataquality[table]["total_rows"] = total_rows

            # get total rows deleted by validation
            total_rows_deleted_by_validation = 0
            sql_query = f"""select count(*) as total_rows 
                        from {table}
                        where _deleted_by_source = 0
                            and _deleted_by_validation = 1"""
            df = spark.sql(sql_query)
            if not df.isEmpty():
                total_rows_deleted_by_validation = df.first()[0]
            dataquality[table]["total_rows_deleted_by_validation"] = total_rows_deleted_by_validation
            
            tableutil = TableUtil(table, spark, debug)
            table_properties = tableutil.get_table_properties()
            if property_name in table_properties:
                expectation_str = table_properties[property_name]
                expectation_dict = json.loads(expectation_str)

                for key in expectation_dict:
                    expectation = expectation_dict[key]
                    if expectation["type"] == "row":
                        if key in violation_dict:
                            expectation["violated_rows"] = violation_dict[key]
                        else:
                            expectation["violated_rows"] = 0

                dataquality[table]["expecations"] = expectation_dict

        return dataquality

