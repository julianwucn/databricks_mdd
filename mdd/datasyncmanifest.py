#!/usr/bin/env python
# coding: utf-8

# ## datasyncmanifest
# 
# New notebook

# In[1]:


# import
from pyspark.sql import Row
import datetime as mdt
import re
import logging

from env.mdd.utilhelper import *


# In[2]:


class TableDataSyncManifest:
    source_data_df = None

    def __init__(self, spark, debug = False):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        if debug:
            logger.debug(f"function begin: {function_name}")

        self.spark = spark
        self.logger = logger
        self.debug = debug

        if debug:
            logger.debug(f"function end: {function_name}")
        # function end: init

    # get the to-be-processed data manifest
    def get_data_sync_manifest(self, source_table, destination_table, data_sync_options):
        function_name = "get_data_sync_manifest"

        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"data_sync_options: {data_sync_options}")

        cdf = "delta.enableChangeDataFeed" 
        pk = "mdd.primaryKey"

        # get source table properties and history 
        source_tableutil = TableUtil(source_table, self.spark, self.debug)
        source_table_history_dict = source_tableutil.get_table_history()
        source_table_history_version_min = source_table_history_dict["table_history_version_min"]
        source_table_history_version_max = source_table_history_dict["table_history_version_max"]
        source_table_history_version_cdf = source_table_history_dict["table_history_version_cdf"]
        
        source_table_property_dict = source_tableutil.get_table_properties()
        source_table_cdf_enabled = (True if cdf in source_table_property_dict and source_table_property_dict[cdf] == "true" else False)
        source_table_primarykey = source_table_property_dict[pk]
        if self.debug:
            self.logger.debug(f"source_table_history_dict: {source_table_history_dict}")
            self.logger.debug(f"source_table_property_dict: {source_table_property_dict}")
            self.logger.debug(f"source_table_primarykey: {source_table_primarykey}")
            self.logger.debug(f"source_table_history_version_min: {source_table_history_version_min}")
            self.logger.debug(f"source_table_history_version_max: {source_table_history_version_max}")
            self.logger.debug(f"source_table_history_version_cdf: {source_table_history_version_cdf}")
            self.logger.debug(f"source_table_cdf_enabled: {source_table_cdf_enabled}")

        # get destination table properties
        destination_tableutil = TableUtil(destination_table, self.spark, self.debug)
        destination_table_property_dict = destination_tableutil.get_table_properties()
        destination_table_primarykey = (source_table_property_dict[pk] if pk in source_table_property_dict else None)
        if self.debug:
            self.logger.debug(f"destination_table_property_dict: {destination_table_property_dict}")
            self.logger.debug(f"destination_table_primarykey: {destination_table_primarykey}")

        destination_history_dict = destination_tableutil.get_table_history()
        destination_table_history_timestamp_max = destination_history_dict["table_history_timestamp_max"]
        
        destination_table_source_cdfversion_max_sql = f"select max(_source_cdfversion) as maxversion from {destination_table} where _source_name = '{source_table}'"
        destination_table_source_cdfversion_max_df = self.spark.sql(destination_table_source_cdfversion_max_sql)
        destination_table_source_cdfversion_max = 0
        if not destination_table_source_cdfversion_max_df.isEmpty():
            destination_table_source_cdfversion_max = destination_table_source_cdfversion_max_df.first()[0]
        if self.debug:
            self.logger.debug(f"destination_table_source_cdfversion_max_sql: {destination_table_source_cdfversion_max_sql}")
            self.logger.debug(f"destination_table_source_cdfversion_max: {destination_table_source_cdfversion_max}")

        # get data_sync_mode_final
        data_sync_mode = data_sync_options["data_sync_mode"]
        incremental_by = data_sync_options["incremental_by"]

        if self.debug:
            print(f"data_sync_mode: {data_sync_mode}")

        data_sync_mode_final = data_sync_mode
        if data_sync_mode == "incremental":
            if incremental_by == "cdf":
                if destination_table_source_cdfversion_max is None or destination_table_source_cdfversion_max == 0:
                    # never loaded before, fallback to full mode
                    data_sync_mode_final = "full"
                elif not source_table_cdf_enabled:
                    # CDF not enabled, fallback to backfill
                    data_sync_mode_final = "backfill"
                elif destination_table_source_cdfversion_max < source_table_history_version_max:
                    # test failed, fallback to backfill
                    try:
                        test_df = self.spark.sql(f"select 1 from table_changes('{source_table}', {destination_table_source_cdfversion_max + 1}, {source_table_history_version_max}) limit 1 ")
                        test_df_isempty = test_df.isEmpty()
                    except:
                        data_sync_mode_final = "backfill"

            elif incremental_by == "timestamp":
                if destination_table_history_timestamp_max is None or destination_table_history_timestamp_max == 0:
                    # never loaded before, fallback to full mode
                    data_sync_mode_final = "full"

            else:
                msg = f"incremental_by: {incremental_by}, not defined"
                self.logger.error(msg)
                raise Exception(msg)

        # fallback warning
        self.data_sync_mode_final = data_sync_mode_final

        # regroup the _commit_timestamp by batches
        full_cutoff_startdate = data_sync_options["full_cutoff_startdate"]
        backfill_past_days = data_sync_options["backfill_past_days"]
        if data_sync_mode_final == "backfill" and (backfill_past_days is None or backfill_past_days < 0):
            msg = f"backfill_past_days: {backfill_past_days}, must be greater or equal 0"
            self.logger.error(msg) 
            raise Exception(msg)

        incremental_cdf_version_min = (None if destination_table_source_cdfversion_max is None else destination_table_source_cdfversion_max + 1) 
        incremental_cdf_version_max = source_table_history_version_max
        if self.debug:
            self.logger.debug(f"data_sync_mode_final: {data_sync_mode_final}")
            self.logger.debug(f"full_cutoff_startdate: {full_cutoff_startdate}")
            self.logger.debug(f"backfill_past_days: {backfill_past_days}")
            self.logger.debug(f"incremental_by: {incremental_by}")
            self.logger.debug(f"incremental_cdf_version_min: {incremental_cdf_version_min}")
            self.logger.debug(f"incremental_cdf_version_max: {incremental_cdf_version_max}")

        # get the dataframe of the source data based on the data sync options
        current_date = mdt.date.today() 
        source_data_sql = f"""select *, 
            'upsert' as _change_type, {incremental_cdf_version_max} as _commit_version, _record_timestamp as _commit_timestamp
            from {source_table}"""

        if data_sync_mode_final == "full":
            if full_cutoff_startdate is not None and full_cutoff_startdate != "1900-01-01":
                source_data_sql = f"{source_data_sql} where _record_timestamp >= '{full_cutoff_startdate}'"

        elif data_sync_mode_final == "backfill":
            backfill_cutoff_startdate = current_date  - mdt.timedelta(backfill_past_days)
            source_data_sql = f"{source_data_sql} where _record_timestamp >= '{backfill_cutoff_startdate}'"

        elif data_sync_mode_final == "incremental":
            if incremental_by == "timestamp":
                source_data_sql = f"{source_data_sql} where _record_timestamp >= '{destination_table_history_timestamp_max}'" 
            elif incremental_by == "cdf":
                # overwrite the sql for the incremental based on cdf
                if incremental_cdf_version_min <= incremental_cdf_version_max:
                    source_data_sql = f"""select * from (
                                        select *, row_number() over (partition by _record_uuid order by _commit_timestamp desc) as _row_no
                                        from table_changes('{source_table}', {incremental_cdf_version_min}, {incremental_cdf_version_max})
                                        where _change_type <> 'update_preimage'
                                        ) r where r._row_no = 1
                                        """
                else:
                    source_data_sql = f"{source_data_sql} where 1 = 0 limit 1" 

        else:
            msg = f"data_sync_mode_final: {data_sync_mode_final}, undefined"
            self.logger.error(msg)
            raise Exception(msg)

        if self.debug:
            self.logger.debug(f"source_data_sql: {source_data_sql}")

        source_data_df = self.spark.sql(source_data_sql).drop("_row_no")
         
        if self.debug:
            self.logger.debug(f"source data: {source_data_df.count()}")
            source_data_df.printSchema
            display(source_data_df)

        # for bronze table, remove the corrupt rows
        if "_corrupt_data" in (name.lower() for name in source_data_df.columns):
            source_data_df = source_data_df.filter("_corrupt_data is null or _corrupt_data = ''")
            
            if self.debug:
                self.logger.debug(f"non-corrupt data: {source_data_df.count()}")
                self.logger.debug(f"source_data_df: fitler out the corrupt records")

        # regroup the data by batches based on _commit_timestamp and rows_per_batch
        rows_per_batch = data_sync_options["rows_per_batch"]

        # one _commit_version in incremental by cdf mode should only have one _commit_timestamp
        # so the data of one version should not be split into multiple batches 
        # use the following agg for just in case this do happen
        if data_sync_mode_final == "incremental" and incremental_by == "timestamp":
            source_data_timestamp_df = source_data_df.groupBy("_commit_version").agg(max("_commit_timestamp").alias("_commit_timestamp"), count("*").alias("_total_rows"))
        else:
            # _commit_version is all the same, add it to make the dataframe having the same columns only
            source_data_timestamp_df = source_data_df.groupBy("_commit_version", "_commit_timestamp").agg(count("*").alias("_total_rows"))

        source_data_timestamp_df = source_data_timestamp_df.sort(asc("_commit_timestamp"))
        if self.debug:
            source_data_timestamp_df.show(truncate = False)

        # get the running totals by _commit_timestamp
        #window_running_total = Window.orderBy("_commit_timestamp").rangeBetween(Window.unboundedPreceding, 0)
        #source_data_timestamp_df = source_data_timestamp_df.withColumn("_running_total_rows", sum("_total_rows").over(window_running_total))
        #if self.debug:
        #    source_data_timestamp_df.sort(asc("_commit_timestamp")).show(truncate = False)

        data_sync_manifest_group_list = []
        if not source_data_timestamp_df.isEmpty():
            if (rows_per_batch is None or rows_per_batch <= 0):
                source_data_timestamp_agg_df = source_data_timestamp_df.agg(min("_commit_timestamp"), max("_commit_timestamp"))
                commit_timestamp_start = source_data_timestamp_agg_df.first()[0]
                commit_timestamp_end = source_data_timestamp_agg_df.first()[1]
                data_sync_manifest_group_list.append([commit_timestamp_start, commit_timestamp_end])
            else:
                rows_in_running_total = 0
                commit_timestamp_start = None
                
                source_data_timestamp_count = source_data_timestamp_df.count()
                rwo_no = 0
                for row in source_data_timestamp_df.collect():
                    if commit_timestamp_start is None:
                        commit_timestamp_start = row["_commit_timestamp"]
                    rows_in_running_total += row["_total_rows"]
                    rwo_no += 1

                    # if exceed the threshold or reach the last row, then generate a new batch
                    if rows_in_running_total >= rows_per_batch or rwo_no == source_data_timestamp_count:
                        commit_timestamp_end = row["_commit_timestamp"]
                        data_sync_manifest_group_list.append([commit_timestamp_start, commit_timestamp_end])
                        # reset the start timestamp and running total
                        rows_in_running_total = 0
                        commit_timestamp_start = None

        # return the source data dataframe
        self.source_data_df = source_data_df
        if self.debug:
            self.logger.debug(f"source_data_df: {source_data_df.count()}")
            self.logger.debug(f"source_data_df_schema: {source_data_df.schema}")
            self.logger.debug(f"data_sync_manifest_group_list: {data_sync_manifest_group_list}")
            self.logger.debug(f"function end: {function_name}")

        return data_sync_manifest_group_list
    # function end: get_data_sync_manifest


# In[ ]:


class FileDataSyncManifest:

   def __init__(self, spark, debug = False):
       function_name = "__init__"
       logger_name = f"mdd.{self.__class__.__name__}"
       logger = logging.getLogger(logger_name)

       if debug:
           logger.debug(f"function begin: {function_name}")
       
       self.debug = debug
       self.spark = spark
       self.logger = logger

       if debug:
           logger.debug(f"function end: {function_name}")
   # function end: init

   # get the to-be-processed files
   def get_data_sync_manifest(self, source_file_path, source_file_name_regex, destination_files, data_sync_options):
       function_name = "get_data_sync_manifest"
       
       if self.debug:
           self.logger.debug(f"function begin: {function_name}")
           self.logger.debug(f"source_file_name_regex: {source_file_name_regex}")
           self.logger.debug(f"destination_files: {destination_files}")
           self.logger.debug(f"data_sync_options: {data_sync_options}")
       
       # extract the data sync options
       data_sync_mode = data_sync_options["data_sync_mode"]
       full_cutoff_startdate = data_sync_options["full_cutoff_startdate"]
       full_cutoff_enddate = data_sync_options["full_cutoff_enddate"]
       backfill_mockup_currentdate = data_sync_options["backfill_mockup_currentdate"]
       backfill_past_days = data_sync_options["backfill_past_days"]
       incremental_cutoff_past_days = data_sync_options["incremental_cutoff_past_days"]
       files_per_batch = data_sync_options["files_per_batch"]

       # get source files
       source_files = []
       fileutil = FileUtil(self.debug)
       source_files = fileutil.get_files(source_file_path, source_file_name_regex)
       # convert the list to dataframe
       source_files_df = self.spark.createDataFrame((Row(**x) for x in source_files), "file_name string, file_path string, file_modification_timestamp timestamp, file_size int, file_date string")
       if self.debug:
           self.logger.debug(f"source_files_df: {source_files_df.count()}")
           display(source_files_df.sort(source_files_df.file_name.desc()))
       

       # get destination files
       destination_files_df = self.spark.createDataFrame(destination_files, "file_name string, file_modification_timestamp timestamp")
       if self.debug:
           self.logger.debug(f"destination_files_df: {destination_files_df.count()}")
           display(destination_files_df.sort(destination_files_df.file_name.desc()))


       # determine the to-be-processed source files according to the data sync options
       current_date = mdt.date.today() # get current utc date with out time part

       data_sync_manifest_df = source_files_df
       if data_sync_mode == "full":
           if not (full_cutoff_startdate is None or full_cutoff_startdate == ""):
               data_sync_manifest_df = data_sync_manifest_df.filter(data_sync_manifest_df.file_date >= full_cutoff_startdate)
           if not (full_cutoff_enddate is None or full_cutoff_enddate == ""):
               data_sync_manifest_df = data_sync_manifest_df.filter(data_sync_manifest_df.file_date <= full_cutoff_enddate)

       elif data_sync_mode == "backfill":
           if backfill_mockup_currentdate is None or backfill_mockup_currentdate == "":
               backfill_cutoff_startdate = current_date - mdt.timedelta(backfill_past_days) 
               data_sync_manifest_df = data_sync_manifest_df.filter(data_sync_manifest_df.file_date >= backfill_cutoff_startdate)
           else:
               if isinstance(backfill_mockup_currentdate, str):
                   backfill_currentdate = mdt.datetime.strptime(backfill_mockup_currentdate, '%Y-%m-%d').date()
               else:
                   backfill_currentdate = backfill_mockup_currentdate
               backfill_cutoff_startdate = backfill_currentdate  - mdt.timedelta(backfill_past_days) 
               data_sync_manifest_df = data_sync_manifest_df.filter((data_sync_manifest_df.file_date >= backfill_cutoff_startdate) & (data_sync_manifest_df.file_date <= backfill_currentdate))
           
       elif data_sync_mode == "incremental":
           if not (incremental_cutoff_past_days is None or incremental_cutoff_past_days <= 0):
               incremental_cutoff_startdate = current_date - mdt.timedelta(incremental_cutoff_past_days)
               data_sync_manifest_df = data_sync_manifest_df.filter(data_sync_manifest_df.file_date >= incremental_cutoff_startdate)
               
           data_sync_manifest_df = data_sync_manifest_df.alias("df1").join(destination_files_df.alias("df2"), ["file_name", "file_modification_timestamp"], "left") \
                                                   .filter("df2.file_name is null").select("df1.*") 
       else:
           raise Exception(f"data_sync_mode '{data_sync_mode}' is undefined")

       
       if self.debug:
           display(data_sync_manifest_df.sort(data_sync_manifest_df.file_name.desc()))

       # group the files by batch, the earliest the first
       if files_per_batch is None or files_per_batch <= 0:
           files_per_batch = 1000

       data_sync_manifest_list = data_sync_manifest_df.select("file_path").toPandas()["file_path"].tolist()
       data_sync_manifest_list.sort(reverse=True)

       #replaced by the two lines above, this line will suspend the notebook
       #data_sync_manifest_list = data_sync_manifest_df.sort(data_sync_manifest_df.file_path.asc()) \
       #                                               .rdd.map(lambda x: x.file_path).collect()
       
       data_sync_manifest_group_list = [data_sync_manifest_list[i : i + files_per_batch] for i in range(0, len(data_sync_manifest_list), files_per_batch)]

       # return the total files
       self.data_sync_manifest_files = len(data_sync_manifest_list)
       self.data_sync_manifest_df = data_sync_manifest_df

       if self.debug:
           self.logger.debug(f"data_sync_manifest_group_list: {len(data_sync_manifest_group_list)}")
           self.logger.debug(f"data_sync_manifest_group_list: {data_sync_manifest_group_list}")
           self.logger.debug(f"function end: {function_name}")

       return data_sync_manifest_group_list
   # end function: get_data_sync_manifest

