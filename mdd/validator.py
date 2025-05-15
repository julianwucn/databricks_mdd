#!/usr/bin/env python
# coding: utf-8

# ## validator
# 
# New notebook

# In[ ]:


from env.mdd.metadata import *
from env.mdd.utilhelper import *

import uuid
import json
import datetime as mdt
from pyspark.sql.functions import *
from delta import *
from delta.tables import *
import logging


# In[ ]:


class Validator:

    def __init__(self, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, logger, debug):
        function_name = "__init__"
                
        if debug:
            logger.debug(f"function begin: {function_name}")
            logger.debug(f"metadata_validator_yml: {metadata_validator_yml}")
            logger.debug(f"schema_bronze: {schema_bronze}")
            logger.debug(f"schema_silver: {schema_silver}")
            logger.debug(f"schema_gold: {schema_gold}")

        metadata = Metadata_Validator(metadata_validator_yml, schema_bronze, schema_silver, schema_gold, logger, debug)
        self.validators = metadata.validators
        self.schema_bronze = schema_bronze
        self.schema_silver = schema_silver
        self.schema_gold = schema_gold
        self.spark = spark
        self.logger = logger
        self.debug = debug

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init


# In[ ]:


class RowValidator(Validator):

    def __init__(self, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, debug):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, logger, debug)

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init

    # validate the data 
    def validate(self, table_schema_name, source_data_df):
        function_name = "validate"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"table_schema_name: {table_schema_name}")
            self.logger.debug(f"source_data_df: {source_data_df.count()}")

        tableutil = TableUtil(table_schema_name, self.spark, self.debug)
        expectation_key = "mdd.validator.expectation"

        # check if _change_type in the dataframe, if yes, then we need to skip the rows of "_change_type = 'delete'" for row validation
        expectation_rule_exception = ""
        if "_change_type" in source_data_df.columns:
            expectation_rule_exception = "_change_type = 'delete'"

        # Initialize data frame which will be referenced in the each iteration of the loop
        source_data_df_validated = source_data_df.withColumn("_data_validation_expectations", lit(""))

        # get validators
        for tbl_list in self.validators:
            for tbl in tbl_list.keys():
                if tbl == table_schema_name:
                    validators = tbl_list[tbl]

                    for validator in validators:
                        expectation_name = validator["expectation"]
                        expectation_type = validator["expectation_type"]
                        expectation_rule = validator["expectation_rule"]
                        expectation_violation_action = validator["expectation_violation_action"]
                        expectation_active = validator["active"]
                        # escape single quote when dumping to json
                        expectation_json = json.dumps(validator).replace("'", "''")

                        if expectation_active and expectation_type == "row":
                            self.logger.info(f"validate: {expectation_name}")
                            if self.debug:
                                self.logger.debug(f"expectation: {expectation_name}")
                                self.logger.debug(f"expectation_type: {expectation_type}")
                                self.logger.debug(f"expectation_rule: {expectation_rule}")
                                self.logger.debug(f"expectation_violation_action: {expectation_violation_action}")
                                self.logger.debug(f"Active: {expectation_active}")
                                self.logger.debug(f"expectation_json: {expectation_json}")

                            #skip the rows of "_change_type = 'delete'" 
                            if expectation_rule_exception:
                                expectation_rule = f"({expectation_rule}) OR ({expectation_rule_exception})"

                            if expectation_violation_action == "drop":
                                # physically drop the violated data
                                source_data_df_validated = source_data_df_validated.where(expectation_rule)

                                # to-do: dump the dropped records to somewhere for trouble-shooting 
                                source_data_df_invalid = source_data_df_validated.filter(expectation_rule)
                                source_data_df_invalid_count = source_data_df_invalid.count()
                                if source_data_df_invalid_count > 0:
                                    self.logger.info(f"invalid rows dropped: {source_data_df_invalid_count}")

                            elif expectation_violation_action == "error":
                                # fail the data flow
                                # to-do: dump the failed records to somewhere for trouble-shooting
                                source_data_df_invalid = source_data_df_validated.filter(expectation_rule)
                                source_data_df_invalid_count = source_data_df_invalid.count()
                                if source_data_df_invalid_count > 0:
                                    self.logger.info(f"invalid rows: {source_data_df_invalid_count}")

                                msg = f"some source data violates the expectation {expectation_name}"
                                self.logger.error(msg)
                                raise Exception(msg)
                                

                            elif expectation_violation_action == "warn":
                                # add warning information to the records
                                expr_validation = f"""
                                                    case 
                                                        when _expectation_rule_result = True then _data_validation_expectations  
                                                        when _expectation_rule_result = False and (_data_validation_expectations IS NULL OR _data_validation_expectations = '') then '{expectation_name}'
                                                        else concat(_data_validation_expectations, ', ', '{expectation_name}')
                                                    end"""
                                source_data_df_validated = source_data_df_validated \
                                    .withColumn("_expectation_rule_result", expr(expectation_rule)) \
                                    .withColumn("_data_validation_expectations_new", expr(expr_validation)) \
                                    .drop("_data_validation_expectations") \
                                    .drop("_expectation_rule_result") \
                                    .withColumnRenamed("_data_validation_expectations_new", "_data_validation_expectations")
                                
                            else:
                                msg = f"The violation action '{expectation_violation_action}' of expectation '{expectation_name}' is undefined."
                                self.logger.error(msg)
                                raise Exception(msg)

                            if self.debug: 
                                self.logger.debug(f"validated result: {source_data_df_validated.count()}")
                        
                            # log the expectation result
                            validation_timestamp = mdt.datetime.now()
                            validation_timestamp_str = validation_timestamp.strftime("%Y-%m-%d, %H:%M:%S.%f")
                            expectation_result = {"type": expectation_type, "timestamp": validation_timestamp_str, "violated_rows": None}
                            tableutil.set_table_property_dict(expectation_key, expectation_name, expectation_result)

                            #self.logger.info(f"validate end: {expectation_name}")

                    # end for 
                # end if
            #end for tbl
        #end for tbl_list

        if self.debug: 
            self.logger.debug(f"function end: {function_name}")

        return source_data_df_validated
    # function end: validate   


# In[ ]:


class TableValidator(Validator):

    def __init__(self, metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, debug):
        function_name = "__init__"
        logger_name = f"mdd.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)

        if debug:
            logger.debug(f"function begin: {function_name}")

        super().__init__(metadata_validator_yml, schema_bronze, schema_silver, schema_gold, spark, logger, debug)

        if debug:
            logger.debug(f"function end: {function_name}")
    # function end: init

    # validate the data 
    def validate(self, table_schema_name, table_full_path):
        function_name = "validate"
        if self.debug:
            self.logger.debug(f"function begin: {function_name}")
            self.logger.debug(f"table_schema_name: {table_schema_name}")

        # validate
        tableutil = TableUtil(table_schema_name, self.spark, self.debug)
        table_properties = tableutil.get_table_properties()
        expectation_key = "mdd.validator.expectation"
        expectation_dict = {}
        if expectation_key in table_properties:
            expectation_str = table_properties[expectation_key]
            expectation_dict = json.loads(expectation_str)

        # get validators
        for tbl_list in self.validators:
            for tbl in tbl_list.keys():
                if tbl == table_schema_name:
                    validators = tbl_list[tbl]
                    for validator in validators:
                        expectation_name = validator["expectation"]
                        expectation_type = validator["expectation_type"]
                        expectation_rule = validator["expectation_rule"].replace("<bronze>", self.schema_bronze).replace("<silver>", self.schema_silver).replace("<gold>", self.schema_gold)
                        expectation_violation_action = validator["expectation_violation_action"]
                        expectation_active = validator["active"]
                        # escape single quote when dumping to json
                        expectation_json = json.dumps(validator).replace("'", "''")
                        if self.debug:
                            self.logger.debug(f"expectation: {expectation_name}")
                            self.logger.debug(f"expectation_type: {expectation_type}")
                            self.logger.debug(f"expectation_rule: {expectation_rule}")
                            self.logger.debug(f"expectation_violation_action: {expectation_violation_action}")
                            self.logger.debug(f"Active: {expectation_active}")
                            self.logger.debug(f"expectation_json: {expectation_json}")

                        if expectation_active and expectation_type == "table":
                            expectation_violation_action_drop_script = validator["expectation_violation_action_drop_script"]
                            if self.debug:
                                self.logger.debug(f"expectation_violation_action_drop_script: {expectation_violation_action_drop_script}")

                            # get the last modified timestamp of the table
                            table_history_dict = tableutil.get_table_history()
                            table_history_timestamp_max = table_history_dict["table_history_timestamp_max"]

                            # get the last validated timestamp
                            last_validation_timestamp = None
                            if expectation_name in expectation_dict:
                                property_value_str = expectation_dict[expectation_name]["timestamp"]
                                last_validation_timestamp = mdt.datetime.strptime(property_value_str, "%Y-%m-%d, %H:%M:%S.%f") 

                            if self.debug:
                                self.logger.debug(f"table_history_dict: {table_history_dict}")
                                self.logger.debug(f"table_history_timestamp_max: {table_history_timestamp_max}")
                                self.logger.debug(f"expectation_dict: {expectation_dict}")
                                self.logger.debug(f"last_validation_timestamp: {last_validation_timestamp}")
                            
                            # run the validator only when there are data changes after the last validation
                            if table_history_timestamp_max is None or last_validation_timestamp is None or table_history_timestamp_max > last_validation_timestamp:
                                self.logger.info(f"validate: {expectation_name}")

                                validated_df = self.spark.sql(expectation_rule)
                                violated_rows = 0
                                if not validated_df.isEmpty():
                                    violated_rows = validated_df.agg(sum("_violated_rows")).collect()[0][0]

                                if violated_rows > 0:
                                    if expectation_violation_action == "error":
                                        msg = f"some source data violates the expectation {expectation_name}"
                                        self.logger.error(msg)
                                        raise Exception(msg)
                                        
                                    elif expectation_violation_action == "warn":
                                        pass

                                    elif expectation_violation_action == "drop":
                                        if expectation_violation_action_drop_script is not None or expectation_violation_action_drop_script != "":
                                            expectation_violation_action_drop_script_str = expectation_violation_action_drop_script.replace("<bronze>", self.schema_bronze).replace("<silver>", self.schema_silver).replace("<gold>", self.schema_gold)
                                            if self.debug:
                                                self.logger.debug(f"expectation_violation_action_drop_script_str: {expectation_violation_action_drop_script_str}")
                                            
                                            drop_key = "_record_uuid"
                                            drop_df = self.spark.sql(expectation_violation_action_drop_script_str)
                                            drop_list = list(drop_df.select(drop_key).toPandas()[drop_key])
                                            if self.debug:
                                                self.logger.debug(f"drop_list: {drop_list}")

                                            dest_df = self.spark.read.table(table_schema_name).filter(col(drop_key).isin(drop_list)) \
                                                        .withColumn("_deleted_by_validation", lit(True)) \
                                                        .withColumn("_record_timestamp",lit(current_timestamp())) 

                                            destination_data_dt = DeltaTable.forPath(self.spark, table_full_path)
                                            # merge the data
                                            target_alias = "target"
                                            source_alias = "source"
                                            merge_join_condition = f"{target_alias}.{drop_key} = {source_alias}.{drop_key}"
                                            merge_update_condition = f"{target_alias}._deleted_by_validation <> {source_alias}._deleted_by_validation or {target_alias}._deleted_by_validation is null"
                                            merge_update_set = {f"{target_alias}._deleted_by_validation": f"{source_alias}._deleted_by_validation", f"{target_alias}._record_timestamp": f"{source_alias}._record_timestamp"}
                                            (
                                                destination_data_dt.alias(target_alias)
                                                                    .merge(source = dest_df.alias(source_alias), condition = merge_join_condition)
                                                                    .whenMatchedUpdate(condition = merge_update_condition, set = merge_update_set)
                                                                    .execute()
                                            )
                                        
                                    else:
                                        msg = f"The violation action '{expectation_violation_action}' of expectation '{expectation_name}' is undefined."
                                        self.logger.error(msg)
                                        raise Exception(msg)
                                    
                                # log the expectation result
                                validation_timestamp = mdt.datetime.now()
                                validation_timestamp_str = validation_timestamp.strftime("%Y-%m-%d, %H:%M:%S.%f")
                                expectation_result = {"type": expectation_type, "timestamp": validation_timestamp_str, "violated_rows": violated_rows}
                                tableutil.set_table_property_dict(expectation_key, expectation_name, expectation_result)
                            
                            #self.logger.info(f"validate end: {expectation_name}")

                    # end for 
                # end if
            # end for tbl
        # end for tbl_list

        if self.debug: 
            self.logger.debug(f"function end: {function_name}")

    # function end: validate

