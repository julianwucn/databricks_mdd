#!/usr/bin/env python
# coding: utf-8

# ## mddhelper
# 
# New notebook

# In[45]:


from delta.tables import DeltaTable
import datetime as mdt
from env.mdd.metadata import *
from pyspark.sql import *


# In[61]:


# update mdd.etl_job
def etl_job_update(
    lakehouse_guid,
    job_run_id,
    job_name,
    job_start_timestamp,
    job_end_timestamp,
    job_status,
    error_message
):
    path = f"/{lakehouse_guid}/Tables/mdd/etl_job"
    log_timestamp = mdt.datetime.utcnow()

    data = [
        (
            job_run_id,
            job_name,
            job_start_timestamp,
            job_end_timestamp,
            job_status,
            error_message,
            log_timestamp
        )
    ]
    spark_session = SparkSession.builder.getOrCreate()
    df = spark_session.createDataFrame(
        data,
        "job_run_id string, job_name string, job_start_timestamp timestamp, job_end_timestamp timestamp, job_status string, error_message string, log_timestamp timestamp",
    )
    dt = DeltaTable.forPath(spark_session, path)
    (
        dt.alias("t")
        .merge(
            df.alias("s"),
            f"s.job_run_id = t.job_run_id and s.job_name = t.job_name and t.job_name = '{job_name}'",
        )
        .whenNotMatchedInsertAll()
        .whenMatchedUpdate(
            set={
                "job_end_timestamp": "s.job_end_timestamp",
                "job_status": "s.job_status",
                "error_message": "s.error_message",
                "log_timestamp": "s.log_timestamp"
            }
        )
        .execute()
    )


# In[ ]:


# update mdd.etl_job_tasks
def etl_job_tasks_update(
    lakehouse_guid,
    task_run_id,
    task_name,
    task_start_timestamp,
    task_end_timestamp,
    task_status,
    parent_task_run_id,
    job_run_id,
    error_message
):
    path = f"/{lakehouse_guid}/Tables/mdd/etl_job_tasks"
    log_timestamp = mdt.datetime.utcnow()

    data = [
        (
            task_run_id,
            task_name,
            task_start_timestamp,
            task_end_timestamp,
            task_status,
            parent_task_run_id,
            job_run_id,
            error_message,
            log_timestamp
        )
    ]
    spark_session = SparkSession.builder.getOrCreate()
    df = spark_session.createDataFrame(
        data,
        "task_run_id string, task_name string, task_start_timestamp timestamp, task_end_timestamp timestamp, task_status string, parent_task_run_id string, job_run_id string, error_message string, log_timestamp timestamp",
    )
    dt = DeltaTable.forPath(spark_session, path)
    (
        dt.alias("t")
        .merge(
            df.alias("s"),
            f"s.task_run_id = t.task_run_id and s.task_name = t.task_name and t.task_name = '{task_name}'",
        )
        .whenNotMatchedInsertAll()
        .whenMatchedUpdate(
            set={
                "task_end_timestamp": "s.task_end_timestamp",
                "task_status": "s.task_status",
                "error_message": "s.error_message",
                "log_timestamp": "s.log_timestamp"
            }
        )
        .execute()
    )


# In[ ]:


# update mdd.etl_job_files
def etl_job_files_update(
    file_run_id,
    file_name,
    file_timestamp,
    file_date,
    file_path,
    table_name,
    data_sync_mode,
    data_write_mode,
    rows_read,
    rows_onboarded,
    task_run_id,
    job_run_id
):
    log_timestamp = mdt.datetime.utcnow()

    data = [
        (
            file_run_id,
            file_name,
            file_timestamp,
            file_date,
            file_path,
            file_rows,
            table_name,
            data_sync_mode,
            data_write_mode,
            rows_read,
            rows_onboarded,
            task_run_id,
            job_run_id,
            log_timestamp
        )
    ]
    spark_session = SparkSession.builder.getOrCreate()
    df = spark_session.createDataFrame(
        data,
        "file_run_id string, file_name string, file_timestamp timestamp, file_date string, file_path string, table_name string, data_sync_mode string, data_write_mode string, rows_read long, rows_onboarded long, task_run_id string , job_run_id string , log_timestamp timestamp",
    )
    df.write.mode("append").saveAsTable("mdd.etl_job_files")


# In[ ]:


# update mdd.etl_job_tables
def etl_job_tables_update(    
   table_run_id              
   ,table_name               
   ,data_sync_mode  
   ,data_write_mode         
   ,source_table_name        
   ,source_table_timestamp   
   ,source_table_cfversion   
   ,rows_read                
   ,batches                  
   ,batch                    
   ,rows_in_batch            
   ,rows_inserted            
   ,rows_updated             
   ,rows_deleted             
   ,read_start_timestamp     
   ,read_end_timestamp       
   ,transform_start_timestamp
   ,transform_end_timestamp  
   ,write_start_timestamp    
   ,write_end_timestamp      
   ,task_run_id              
   ,job_run_id               
   ,log_timestamp            
          
):
    data = [(    
        table_run_id              
        ,table_name               
        ,data_sync_mode  
        ,data_write_mode         
        ,source_table_name        
        ,source_table_timestamp   
        ,source_table_cfversion   
        ,rows_read                
        ,batches                  
        ,batch                    
        ,rows_in_batch            
        ,rows_inserted            
        ,rows_updated             
        ,rows_deleted             
        ,read_start_timestamp     
        ,read_end_timestamp       
        ,transform_start_timestamp
        ,transform_end_timestamp  
        ,write_start_timestamp    
        ,write_end_timestamp      
        ,task_run_id              
        ,job_run_id               
        ,log_timestamp            
          
    )]

    schema = """
    table_run_id                   string
    ,table_name                    string
    ,data_sync_mode                string
    ,data_write_mode               string
    ,source_table_name             string
    ,source_table_timestamp        timestamp
    ,source_table_cfversion        long
    ,rows_read                     long
    ,batches                       long
    ,batch                         string
    ,rows_in_batch                 long
    ,rows_inserted                 long
    ,rows_updated                  long
    ,rows_deleted                  long
    ,read_start_timestamp          timestamp
    ,read_end_timestamp            timestamp
    ,transform_start_timestamp     timestamp
    ,transform_end_timestamp       timestamp
    ,write_start_timestamp         timestamp
    ,write_end_timestamp           timestamp
    ,task_run_id                   string
    ,job_run_id                    string
    ,log_timestamp                 timestamp
    """

    spark_session = SparkSession.builder.getOrCreate()

    df = spark_session.createDataFrame(data, schema)
    df.write.mode("append").saveAsTable("mdd.etl_job_tables")

