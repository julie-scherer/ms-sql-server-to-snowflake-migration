"""
### Load CREO data to Snowflake 
Scheduled to run every morning at 6AM EST
This DAG copies data for the below listed tables based on table type (increamental, full-load) to Snowflake.
"""

import os
import logging
from datetime import datetime, timedelta
import pendulum
from pathlib import Path
import re
import pandas as pd

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions

# - - - - - - - - - SETTINGS - - - - - - - - -

mssql_hook = MsSqlHook(mssql_conn_id="awsmssql_creosql_creo_conn")
s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

# Data driven scheduling datasets 
dataset_file = "include/datasets/Staging_CREO_Dataset.txt"
dataset_obj = Dataset(dataset_file)

fullTableList = {
    # >> #1 Custom SQL Loads 
    "CREO_MESSAGE_HIST": {"sql": "COPY_CREO_MESSAGE.sql", "key_column": "MESSAGE_KEY", "keys": ["CONTAINER_KEY", "TEMPLATE_KEY", "SEND_AFTER_MESSAGE_KEY", "DELIVERY_STATUS_KEY", "IN_REPLY_TO_MESSAGE_KEY", "COMMUNICATION_MAILING_KEY", "MESSAGE_KEY", "DATASET_ROW_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    # << #1 Custom SQL Loads 

    ## >> #2 Incremental load using DATE column
    "CREO_APPROVALREQUEST_HIST": {"sql": None, "key_column": "APPROVAL_REQUEST_KEY", "keys": ["PACKAGE_KEY", "APPROVAL_REQUEST_KEY"], "table_filtered_by": "ENTERED_AT"}, 
    "CREO_CAMPAIGN_HIST": {"sql": None, "key_column": "CAMPAIGN_KEY", "keys": ["CONTAINER_KEY", "CAMPAIGN_TYPE_KEY", "CAMPAIGN_KEY", "SEED_LIST_DATASET_KEY", "DATASET_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_COMMUNICATION_HIST": {"sql": None, "key_column": "COMMUNICATION_KEY", "keys": ["CAMPAIGN_KEY", "COMMUNICATION_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_COMMUNICATIONMAILING_HIST": {"sql": None, "key_column": "COMMUNICATION_MAILING_KEY", "keys": ["COMMUNICATION_MAILING_KEY", "COMMUNICATION_KEY"], "table_filtered_by": "DATE_COMPLETED"}, 
    "CREO_CONFIGHISTORY_HIST": {"sql": None, "key_column": "CONFIG_HISTORY_KEY", "keys": ["CONFIG_HISTORY_KEY", "CONFIG_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_CONTACT_HIST": {"sql": None, "key_column": "CONTACT_KEY", "keys": ["CONTACT_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_CONTAINER_HIST": {"sql": None, "key_column": "CONTAINER_KEY", "keys": ["CONTAINER_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_DATASET_HIST": {"sql": None, "key_column": "DATASET_KEY", "keys": ["CONTAINER_KEY", "DATASET_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_DEADMESSAGES_HIST": {"sql": None, "key_column": "MESSAGE_KEY", "keys": ["CONTAINER_KEY", "TEMPLATE_KEY", "SEND_AFTER_MESSAGE_KEY", "DELIVERY_STATUS_KEY", "IN_REPLY_TO_MESSAGE_KEY", "COMMUNICATION_MAILING_KEY", "MESSAGE_KEY", "DATASET_ROW_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_DEADMESSAGES2_HIST": {"sql": None, "key_column": "MESSAGE_KEY", "keys": ["CONTAINER_KEY", "TEMPLATE_KEY", "SEND_AFTER_MESSAGE_KEY", "DELIVERY_STATUS_KEY", "IN_REPLY_TO_MESSAGE_KEY", "COMMUNICATION_MAILING_KEY", "MESSAGE_KEY", "DATASET_ROW_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_LOG_HIST": {"sql": None, "key_column": "LOG_KEY", "keys": ["CONTAINER_KEY", "PACKAGE_KEY", "TEMPLATE_KEY", "CAMPAIGN_KEY", "COMMUNICATION_KEY", "MESSAGE_KEY", "LOG_KEY", "RULE_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_MESSAGEDELIVERYSTATUS_HIST": {"sql": None, "key_column": "MESSAGE_DELIVERY_STATUS_KEY", "keys": ["MESSAGE_KEY", "MESSAGE_DELIVERY_STATUS_KEY", "DELIVERY_STATUS_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_MESSAGESTATUSQUEUE_HIST": {"sql": None, "key_column": "MESSAGE_STATUS_QUEUE_KEY", "keys": ["MESSAGE_STATUS_QUEUE_KEY", "CONTAINER_KEY"], "table_filtered_by": "ENTERED_AT"}, 
    "CREO_PACKAGE_HIST": {"sql": None, "key_column": "PACKAGE_KEY", "keys": ["PACKAGE_KEY", "CONTAINER_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_RULE_HIST": {"sql": None, "key_column": "RULE_KEY", "keys": ["CONTAINER_KEY", "RULE_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_TEMPLATE_HIST": {"sql": None, "key_column": "TEMPLATE_KEY", "keys": ["TEMPLATE_KEY", "BASE_TEMPLATE_KEY"], "table_filtered_by": "DATE_ENTERED",  "copy_into": "COPY_CREO_TEMPLATE.sql"}, 
    "CREO_USER_HIST": {"sql": None, "key_column": "USER_KEY", "keys": ["USER_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_WEBHOOK_HIST": {"sql": None, "key_column": "WEBHOOK_KEY", "keys": ["CONTAINER_KEY", "WEBHOOK_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    # << #2 Incremental load using Date_Entered

    # >> #3 Incremental load using key
    "CREO_APPROVALREQUESTITEM_HIST": {"sql": None, "key_column": "APPROVAL_REQUEST_ITEM_KEY", "keys": ["TEMPLATE_KEY", "APPROVAL_REQUEST_KEY", "APPROVAL_REQUEST_ITEM_KEY"], "table_filtered_by": None}, 
    "CREO_CAMPAIGNTYPE_HIST": {"sql": None, "key_column": "CAMPAIGN_TYPE_KEY", "keys": ["CAMPAIGN_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_CONFIG_HIST": {"sql": None, "key_column": "CONFIG_KEY", "keys": ["CONFIG_KEY"], "table_filtered_by": None}, 
    "CREO_CONTACTTYPE_HIST": {"sql": None, "key_column": "CONTACT_TYPE_KEY", "keys": ["CONTACT_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_DATASETCOLUMN_HIST": {"sql": None, "key_column": "DATASET_COLUMN_KEY", "keys": ["DATASET_COLUMN_KEY"], "table_filtered_by": None}, 
    "CREO_DATASETROW_HIST": {"sql": None, "key_column": "DATASET_ROW_KEY", "keys": ["DATASET_ROW_KEY", "DATASET_KEY"], "table_filtered_by": None}, 
    "CREO_DATASETVALUE_HIST": { "sql": None,  "key_column": "DATASET_VALUE_KEY",  "keys": [ "DATASET_VALUE_KEY" ],  "table_filtered_by": None }, 
    "CREO_DATASOURCE_HIST": {"sql": None, "key_column": "DATASOURCE_KEY", "keys": ["DATASOURCE_KEY"], "table_filtered_by": None}, 
    "CREO_DELIVERYSTATUS_HIST": {"sql": None, "key_column": "DELIVERY_STATUS_KEY", "keys": ["DELIVERY_STATUS_KEY"], "table_filtered_by": None}, 
    "CREO_EMOJI_HIST": {"sql": None, "key_column": "EMOJI_KEY", "keys": ["EMOJI_KEY"], "table_filtered_by": None}, 
    "CREO_FOLDER_HIST": {"sql": None, "key_column": "FOLDER_KEY", "keys": ["CONTAINER_KEY", "PARENT_FOLDER_KEY", "FOLDER_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGECONTACT_HIST": {"sql": None, "key_column": "MESSAGE_CONTACT_KEY", "keys": ["CONTACT_KEY", "MESSAGE_KEY", "MESSAGE_CONTACT_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGECONTACTTYPE_HIST": {"sql": None, "key_column": "MESSAGE_CONTACT_TYPE_KEY", "keys": ["MESSAGE_CONTACT_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGECONTACTV2_HIST": {"sql": None, "key_column": "MESSAGE_CONTACT_KEY", "keys": ["CONTACT_KEY", "MESSAGE_KEY", "MESSAGE_CONTACT_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGEPART_HIST": {"sql": None, "key_column": "MESSAGE_PART_KEY", "keys": ["MESSAGE_PART_KEY", "MESSAGE_KEY"], "table_filtered_by": None,  "copy_into": "COPY_CREO_MESSAGEPART.sql"}, 
    "CREO_MESSAGEPARTV2_HIST": { "sql": None,  "key_column": "MESSAGE_PART_KEY",  "keys": [ "MESSAGE_PART_KEY",  "MESSAGE_KEY" ],  "table_filtered_by": None ,  "copy_into": "COPY_CREO_MESSAGEPART.sql"}, 
    "CREO_MESSAGETYPE_HIST": {"sql": None, "key_column": "MESSAGE_TYPE_KEY", "keys": ["MESSAGE_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_PARAMETER_HIST": {"sql": None, "key_column": "PARAMETER_KEY", "keys": ["PARAMETER_KEY", "DATASOURCE_KEY"], "table_filtered_by": None}, 
    "CREO_TEMPLATETYPE_HIST": {"sql": None, "key_column": "TEMPLATE_TYPE_KEY", "keys": ["TEMPLATE_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_TEMPMESSAGE_HIST": {"sql": None, "key_column": "TEMP_MESSAGE_KEY", "keys": ["TEMP_MESSAGE_KEY"], "table_filtered_by": None}, 
    # << #3 Incremental load using key

    # >> #4 Partial full load using MAX key
    "CREO_DATASETCELL_HIST": {"sql": None, "select_key_column": True, "keys": ["DATASET_ROW_KEY", "DATASET_COLUMN_KEY", "DATASET_VALUE_KEY"], "table_filtered_by": None}, # new entries should come with a new dataset_row_key
    "CREO_FOLDERMESSAGE_HIST": {"sql": None, "select_key_column": True, "keys": ["MESSAGE_KEY", "FOLDER_KEY"], "table_filtered_by": None},  # FolderMessage will have new message_key values 
    "CREO_PACKAGETEMPLATE_HIST": {"sql": None, "select_key_column": True, "keys": ["PACKAGE_KEY", "TEMPLATE_KEY"], "table_filtered_by": None},  # PackageTemplate should always have new package_key values
    "CREO_TEMPLATERULE_HIST": {"sql": None, "select_key_column": True, "keys": ["TEMPLATE_KEY", "RULE_KEY"], "table_filtered_by": None}, # TemplateRule should always have new template_key values
    # << #4 Partial full load using MAX key 
    
    # >> #5 Full load
    "CREO_FOLDERCONTACT_HIST": {"sql": None, "key_column": None, "keys": ["CONTACT_KEY", "FOLDER_KEY"], "table_filtered_by": None}, # there is no good way of only getting new records for FolderContact
    "CREO_GLOBAL_HIST": {"sql": None, "key_column": None, "keys": ["APP_VERSION"], "table_filtered_by": None}, 
    # << #5 Full load
}

# - - - - - - - - STAGING DAG - - - - - - - - -

staging_dir = f"/usr/local/airflow/include/sql/Staging_CREO"

## Default DAG args
default_args = {
    'owner': 'Data Engineering',
    "retries":  0,
    "retry_delay":  timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
    "on_failure_callback":  ms_teams_callback_functions.failure_callback
}

## Initialize the DAG
@dag(
    "Staging_CREO",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    catchup=False,
    schedule="0 6 * * *",  # 6AM EST
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    tags=["Author: Julie Scherer"],
    template_searchpath=f"include/sql/Staging_CREO/",
    default_args=default_args
)
def Staging_CREO():
    start, mid, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "mid", "end"]]

    @task(multiple_outputs=True)
    def get_runtime_params(table_name, ds=None) -> dict:
        """
        Fetches runtime parameters and sets them in the context dictionary.
                
        Parameters:
            table_name (str): The name of the Snowflake table to export data to.
            ds (str, optional): The date string in the format "YYYY-MM-DD". Defaults to None.
            
        Returns:
            dict: A dictionary containing the runtime parameters.
        """
        trunc_table_name = table_name.replace('CREO_','').replace('_HIST','')

        output = sf_hook.get_first(f"SELECT ETL.COPYSELECT('STG','{table_name.upper()}',3)")
        columns = output[f"ETL.COPYSELECT('STG','{table_name.upper()}',3)"]

        aod = datetime.strptime(ds, "%Y-%m-%d")
        s3_bucket_name = Variable.get("s3_etldata_bucket_var")
        
        s3_file_path = aod.strftime(f"CREO/%Y/%m/%d/{trunc_table_name}")
        file_name = aod.strftime(f"{trunc_table_name}_%Y%m%d")
        file_format = 'CREO_CSV_PIPE_SH0_EON'

        runtime_params = {
            "table_name":  table_name,
            "trunc_table_name":  trunc_table_name,
            "columns":  columns,
            "aod":  aod,
            "s3_bucket_name":  s3_bucket_name,
            "s3_file_path":  s3_file_path,
            "file_name":  file_name,
            "file_format":  file_format,
        }
        logging.info(f"Runtime Parameters: \n\n{runtime_params}\n")

        return runtime_params

    @task
    def copy_mssql_to_s3(runtime_params, table):
        """
        Loads the ZIP file obtained from the previous task to the specified S3 bucket.
        """
        logging.info(f"<< Runtime parameters >> \n{runtime_params}")

        table_name = runtime_params.get("table_name")
        trunc_table_name = runtime_params.get("trunc_table_name")
        file_name = runtime_params.get("file_name")
        s3_bucket_name = runtime_params.get("s3_bucket_name")
        s3_file_path = runtime_params.get("s3_file_path")
        aod = runtime_params.get("aod")

        ## Get the MSSQL query based on the runtime parameters
        sql_file = fullTableList[table].get("sql")
        table_filtered_by = fullTableList[table].get("table_filtered_by")
        key_column = fullTableList[table].get("key_column")
        select_key_column = fullTableList[table].get("select_key_column", False) # set this value to False if not found
        keys = fullTableList[table].get("keys")
        
        ## Assign the Pandas DataFrames to empty values for error catching later
        df = pd.DataFrame()
        chunks = None
        
        ## << #1 Custom SQL >>
        # If there's a custom SQL file...
        if sql_file != None and table_filtered_by != None:
            ## Reset Snowflake table
            sfsql_query = f"""
                DELETE FROM STG.{table_name} WHERE CAST({table_filtered_by} AS DATE) = TO_DATE('{aod}');
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            sf_hook.run(sfsql_query)
            
            ## Read data from MSSQL
            logging.info(f"<< #1 Running custom SQL query >>")
            with open(f"{staging_dir}/{sql_file}", "r") as file:
                mssql_query = file.read().format(asOfDate=aod)
            logging.info(f"Getting results from MSSQL: \n{mssql_query}")
            df = mssql_hook.get_pandas_df(mssql_query)
        
        ## << #2 Incremental load using DATE column >>
        # If there's a date column to filter by...
        elif table_filtered_by != None:
            ## Reset Snowflake table
            logging.info(f"<< Found date column: `{table_filtered_by}` >>")
            sfsql_query = f"""
                DELETE FROM STG.{table_name} WHERE ASOFDATE = TO_DATE('{aod}');
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            sf_hook.run(sfsql_query)
            
            ## Read data from MSSQL
            logging.info(f"<< #2 Incremental load using `{table_filtered_by}` >>")
            mssql_query = f"""
                SELECT * FROM [dbo].[{trunc_table_name}] WHERE CAST([{table_filtered_by}] AS DATE) = '{aod}';
            """
            logging.info(f"Getting results from MSSQL: \n{mssql_query}")
            df = mssql_hook.get_pandas_df(mssql_query)
        
        ## << #3 Running with KEY_COLUMN >>
        # If there's a key column to filter by...
        elif key_column != None:
            ## Reset Snowflake table
            logging.info(f"<< Found key column: `{key_column}` >>")
            sfsql_query = f"""
                DELETE FROM STG.{table_name} WHERE METADATAFILENAME = '{s3_file_path}/{file_name}';
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            sf_hook.run(sfsql_query)

            ## Read data from MSSQL
            logging.info(f"<< #3 Incremental load using `{key_column}` >>")
            snowflake_query = f"""
                SELECT MAX({key_column}) FROM STG.{table_name}
            """
            max_value_df = sf_hook.get_pandas_df(snowflake_query)
            max_value = max_value_df.iloc[0, 0]

            #Logging the max value. A max value of 0 means that the table in snowflake is currently empty
            if max_value == None:
                max_value = 0
            logging.info(max_value)
            
            mssql_query = f"""
                SELECT * FROM [dbo].[{trunc_table_name}] WHERE [{key_column}] > {max_value};
            """
            logging.info(f"Getting results from MSSQL: \n{mssql_query}")
            df = mssql_hook.get_pandas_df(mssql_query)
        
        ## !<< #4 Partial full load using max key in KEYS >>
        # If there's a key column to filter by...
        elif keys and select_key_column == True:
            ## The first value in the keys list is the key that should always be new (according to the DB owner)
            key_column = keys[0]

            ## Getting the max value for the first key in keys 
            logging.info(f"<< #4 Partial full load using MAX key value : `{key_column}` >>")
            snowflake_max = f"""
                SELECT MAX({key_column}) FROM STG.{table_name}
            """
            max_value_df = sf_hook.get_pandas_df(snowflake_max)
            max_value = max_value_df.iloc[0, 0]
            if max_value == None:
                max_value = 0
            logging.info(f"MAX({key_column}) = {max_value}")

            ## Reset Snowflake table
            sfsql_query = f"""
                DELETE FROM STG.{table_name} WHERE {key_column} = {max_value};
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            sf_hook.run(sfsql_query)

            ## Read data from MSSQL, starting at the max value and on
            mssql_query = f"""
                SELECT * FROM [dbo].[{trunc_table_name}] WHERE [{key_column}] >= {max_value};
            """
            logging.info(f"Getting results from MSSQL: \n{mssql_query}")
            
            chunks = mssql_hook.get_pandas_df_by_chunks(sql=mssql_query, chunksize=20000)

        ## << #5 Full Load >>
        else:
            ## Reset Snowflake table
            sfsql_query = f"""
                DELETE FROM STG.{table_name} WHERE ASOFDATE = to_date('{aod}');
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            sf_hook.run(sfsql_query)

            ## Read data from MSSQL
            mssql_query = f"""
                SELECT * FROM [dbo].[{trunc_table_name}];
            """
            logging.info(f"Getting results from MSSQL: \n{mssql_query}")
        
            chunks = mssql_hook.get_pandas_df_by_chunks(sql=mssql_query, chunksize=20000)

        ## << Loading the data to MSSQL >>
        # - - - - - - - - - - - - - - - - -
        if not df.empty:
            
            # Convert df to CSV bytes
            df_byte = df.to_csv(
                header=False,  # Exclude the column headers from the CSV
                index=False,  # Exclude the row index from the CSV
                sep=f'|',  # Use the pipe symbol as the column separator
                lineterminator=f'\n',
                na_rep='NULL',  # Replace missing values with 'NULL'
                doublequote=True,  # Enable double quoting for values
                quotechar='"',
            ).encode()
            
            # Load df bytes to S3
            s3_hook.load_bytes(
                bytes_data=df_byte,
                bucket_name=s3_bucket_name,
                key=f"inbound/{s3_file_path}/{file_name}",
                replace=True,
            )
            logging.info(f'<< Uploading DataFrame to S3: {file_name} >>')
            return True
        
        elif chunks != None:
            for idx, chunk in enumerate(chunks):
                chunk_idx = idx+1 # Add 1 to the idx value because Python counting starts at 0

                # Convert chunk to CSV bytes
                df_byte = chunk.to_csv(
                    header=False,  # Exclude the column headers from the CSV
                    index=False,  # Exclude the row index from the CSV
                    sep='|',  # Use the pipe symbol as the column separator
                    lineterminator='\n',
                    na_rep='NULL',  # Replace missing values with 'NULL'
                    doublequote=True,  # Enable double quoting for values
                    quotechar='"',
                ).encode()

                # Load chunk bytes to S3
                s3_hook.load_bytes(
                    bytes_data=df_byte,
                    bucket_name=s3_bucket_name,
                    key=f"inbound/{s3_file_path}/{chunk_idx}_{file_name}",
                    replace=True,
                )
                logging.info(f'<< Uploading chunk to S3: {chunk_idx}_{file_name} >>')
            logging.info(f'<< Finished uploading chunks to S3 >>')
            return True

        else:
            logging.info('!!! No results in DataFrame from MSSQL !!!')
            return False

    @task
    def copy_to_snowflake(runtime_params, table):
        """
        Copies data from the S3 bucket to the Snowflake database table specified in the runtime parameters.

        The SQL query is constructed dynamically based on the data and runtime parameters.
        The data is copied using the "COPY INTO" Snowflake SQL command.
        """
        table_name = runtime_params.get("table_name")
        yesterday = runtime_params.get("aod")
        columns = runtime_params.get("columns")
        s3_file_path = runtime_params.get("s3_file_path")
        file_name = runtime_params.get("file_name")
        file_format = runtime_params.get("file_format")

        custom_copy_into = fullTableList[table].get("copy_into")

        if custom_copy_into:
            logging.info(f"<< Running custom copy into query >>")
            with open(f"{staging_dir}/{custom_copy_into}", "r") as file:
                sql_query = file.read().format(
                    yesterday=yesterday,
                    s3_file_path=s3_file_path,
                    file_format=file_format,
                    file_name=file_name
                )
        else:
            sql_query = f"""
                -- \\ {table_name}
                COPY INTO STG.{table_name}
                FROM (
                    SELECT 
                        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{yesterday}'), 
                        {columns} 
                    FROM @ETL.INBOUND/{s3_file_path}/
                )
                FILE_FORMAT = ( 
                    FORMAT_NAME = {file_format}
                ) 
                PATTERN = '.*{file_name}.*'
            """

        sf_hook.run(sql_query)
        logging.info(f'<< Loading staged data to Snowflake >> \n{sql_query}')
        return True


    ## Write to the dataset file to trigger the DQ dag
    @task(outlets=[dataset_obj])
    def write_to_dataset():
        dataset_path = Path(dataset_file)
        dataset_path.touch(exist_ok=True)
        with open(dataset_file, "a") as file:
            file.write("Staging_CREO data load complete")
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    for table in fullTableList:
        with TaskGroup(group_id=f"{table}_Task") as creo_taskgroup:
            table_data = get_runtime_params(table)
            copy_to_s3 = copy_mssql_to_s3(table_data, table)
            table_data >> copy_to_s3  >> copy_to_snowflake(copy_to_s3, table)
            
        start >> creo_taskgroup >> mid 
    mid >> write_to_dataset() >> end

creo_run = Staging_CREO()
