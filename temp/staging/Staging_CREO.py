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
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions

from CREO_Utils import fullTableList, AS_OF_DATE

# - - - - - - - - - SETTINGS - - - - - - - - -

FIELD_DELIMITER = "|"
RECORD_DELIMITER = "\n"
FILE_FORMAT = f"""
    TYPE = CSV 
    COMPRESSION = AUTO 
    FIELD_DELIMITER = '|' 
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 0 
    REPLACE_INVALID_CHARACTERS = TRUE 
    NULL_IF = 'NULL'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = 'NONE'
"""

CHUNK_SIZE = 20000
SQL_DIR = f"/usr/local/airflow/include/sql/Staging_CREO"

# Data driven scheduling datasets 
dataset_file = "include/datasets/Staging_CREO_Dataset.txt"
dataset_obj = Dataset(dataset_file)

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="awsmssql_creosql_creo_conn")
    return MSSQL_HOOK

def get_s3_hook():
    """
    Returns a S3 hook for interacting with S3.
    """
    S3_HOOK = S3Hook(aws_conn_id="aws_s3_conn")
    return S3_HOOK

def get_sf_hook():
    """
    Returns a Snowflake hook for interacting with Snowflake.
    """
    SF_HOOK = SnowflakeHook(snowflake_conn_id="snowflake_default")
    return SF_HOOK

# - - - - - - - - STAGING DAG - - - - - - - - -

## Default DAG args
default_args = {
    'owner': 'Data Engineering',
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
    "on_failure_callback": ms_teams_callback_functions.failure_callback
}

## Initialize the DAG
@dag(
    "Staging_CREO",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    schedule="0 6 * * *",  # 6AM EST
    catchup=False,
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath=f"include/sql/Staging_CREO/",
    default_args=default_args
)
def Staging_DAG():

    @task(multiple_outputs=True)
    def get_runtime_params(table_name, ds=None) -> dict:
        """
        Fetches runtime parameters and sets them in the context dictionary.
        
        Retrieves runtime parameters and generates file and directory paths for data export.
        
        Parameters:
            table_name (str): The name of the Snowflake table to export data to.
            ds (str, optional): The date string in the format "YYYY-MM-DD". Defaults to None.
            
        Returns:
            dict: A dictionary containing the following runtime parameters:
                - table_name (str): The table name without the database prefix and "_HIST".
                - columns (str): The columns to be copied.
                - aod (datetime): The date as a datetime object.
                - s3_bucket_name (str): The name of the S3 bucket for data storage.
                - s3_dir_path (str): The S3 bucket file path where the data will be stored.
                - file_name (str): The name of the file to be created.
                - table_utils (dict): A dictionary containing table utilities.
        """
        get_numbered_columns_query = f"SELECT ETL.COPYSELECT('STG','CREO_{table_name}_HIST',3)"
        query_output = get_sf_hook().get_first(get_numbered_columns_query)
        logging.info(f"COPYSELECT Query Output: {query_output}")
        
        columns = list(query_output.values())[0]
        logging.info(f"Query output value: {columns}")

        aod = datetime.strptime(ds, "%Y-%m-%d")
        s3_bucket_name = Variable.get("s3_etldata_bucket_var")
        s3_dir_path = aod.strftime(f"CREO/%Y/%m/%d/{table_name}")

        file_name = aod.strftime(f"{table_name}_%Y%m%d")

        table_utils = fullTableList.get(table_name)
        logging.info(f"Retrieved table utilities: \n{table_utils}, {type(table_utils)}")

        runtime_params = {
            "table_name": table_name,
            "columns": columns,
            "aod": aod,
            "s3_bucket_name": s3_bucket_name,
            "s3_dir_path": s3_dir_path,
            "file_name": file_name,
            "table_utils": table_utils,
        }
        logging.info(f"Runtime Parameters: \n\n{runtime_params}\n")

        return runtime_params

    @task
    def mssql_to_s3(runtime_params):
        """
        Loads the ZIP file obtained from the previous task to the specified S3 bucket.
        """
        logging.info(f"<< Runtime parameters >> \n{runtime_params}")

        table_name = runtime_params.get("table_name")
        file_name = runtime_params.get("file_name")
        s3_bucket_name = runtime_params.get("s3_bucket_name") # Variable.get("s3_etldata_bucket_var")
        s3_dir_path = runtime_params.get("s3_dir_path")
        aod = runtime_params.get("aod")

        ## Get the MSSQL query based on the runtime parameters
        logging.info(f"Retrieving MSSQL query")
        table_utils = runtime_params.get("table_utils")
        sql_file = table_utils.get('SQL')
        table_filtered_by = table_utils.get('TABLE_FILTERED_BY')
        key_column = table_utils.get('KEY_COLUMN')
        
        ## Assign the df to empty value for error catching later
        df = pd.DataFrame()
        
        ## << #1 Custom SQL >>
        # If there's a custom SQL file...
        # - - - - - - - - - - - - - - - - - - - - - - - - 
        if sql_file and sql_file != 'NULL':
            ## Reset Snowflake table
            sfsql_query = f"""
                DELETE FROM STG.CREO_{table_name}_HIST 
                WHERE CAST(DATE_ENTERED AS DATE) = TO_DATE('{aod}');
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            get_sf_hook().run(sfsql_query)
            
            ## Read data from MSSQL
            logging.info(f"<< #1 Running custom SQL query >>")
            with open(f"{SQL_DIR}/{sql_file}", "r") as file:
                mssql_query = file.read().format(asOfDate=aod)
            df = get_mssql_hook().get_pandas_df(mssql_query)
        
        ## << #2 Incremental load using DATE column >>
        # If there's a date column to filter by...
        # - - - - - - - - - - - - - - - - - - - - - - - - 
        elif table_filtered_by and table_filtered_by != 'NULL':
            ## Reset Snowflake table
            logging.info(f"<< Found date column: `{table_filtered_by}` >>")
            sfsql_query = f"""
                DELETE FROM STG.CREO_{table_name}_HIST 
                WHERE ASOFDATE = TO_DATE('{aod}');
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            get_sf_hook().run(sfsql_query)
            
            ## Read data from MSSQL
            logging.info(f"<< #2 Incremental load using `{table_filtered_by}` >>")
            mssql_query = f"""
                SELECT * FROM [dbo].[{table_name}]
                WHERE CAST([{table_filtered_by}] AS DATE) = '{aod}';
            """
            df = get_mssql_hook().get_pandas_df(mssql_query)
        
        ## << #3 Running with KEY_COLUMN >>
        # If there's a key column to filter by...
        # - - - - - - - - - - - - - - - - - - - - - - - - 
        elif key_column and key_column != 'NULL':
            ## Reset Snowflake table
            logging.info(f"<< Found key column: `{key_column}` >>")
            sfsql_query = f"""
                DELETE FROM STG.CREO_{table_name}_HIST 
                WHERE METADATAFILENAME LIKE 'inbound/{s3_dir_path}/{file_name}%';
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            get_sf_hook().run(sfsql_query)

            ## Read data from MSSQL
            logging.info(f"<< #3 Incremental load using `{key_column}` >>")
            snowflake_query = f"""
                SELECT MAX({key_column}) FROM STG.CREO_{table_name}_HIST
            """
            max_value_df = get_sf_hook().get_pandas_df(snowflake_query)
            max_value = max_value_df.iloc[0, 0]
            max_value = 0 if max_value is None else max_value
            mssql_query = f"""
                SELECT * FROM [dbo].[{table_name}]
                WHERE [{key_column}] > {max_value};
            """
            df = get_mssql_hook().get_pandas_df(mssql_query)

        ## << #3 Full Load >>
        # If none of the above are true...
        # - - - - - - - - - - - - - - - - - - - - - - - - 
        else:
            ## Reset Snowflake table
            logging.info(f"<< Full load using asOfDate = {aod} >> ")
            sfsql_query = f"""
                DELETE FROM STG.CREO_{table_name}_HIST 
                WHERE ASOFDATE = TO_DATE('{aod}');
            """
            logging.info(f"Resetting Snowflake table: \n{sfsql_query}")
            get_sf_hook().run(sfsql_query)

            ## Read data from MSSQL
            logging.info(f"<< #3 Running full load >> ")
            mssql_query = f"SELECT * FROM [dbo].[{table_name}];"
            chunks = get_mssql_hook().get_pandas_df_by_chunks(sql=mssql_query, chunksize=CHUNK_SIZE)
            
            # df_list = [] #! remove
            for idx, chunk in enumerate(chunks):
            #     df_list.append(chunk) #! remove
            # df = pd.concat(df_list) #! remove
                batch_num = idx+1

                # Convert chunk to CSV bytes
                df_byte = chunk.to_csv(
                    header=False,  # Exclude the column headers from the CSV
                    index=False,  # Exclude the row index from the CSV
                    sep=f'{FIELD_DELIMITER}',  # Use the pipe symbol as the column separator
                    lineterminator=f'{RECORD_DELIMITER}',
                    na_rep='NULL',  # Replace missing values with 'NULL'
                    doublequote=True,  # Enable double quoting for values
                    quotechar='"',
                ).encode()

                # Load chunk bytes to S3
                get_s3_hook().load_bytes(
                    bytes_data=df_byte,
                    bucket_name=s3_bucket_name,
                    key=f"inbound/{s3_dir_path}/{batch_num}_{file_name}.csv",
                    replace=True,
                )

                logging.info(f'<< Uploading chunk to S3: {batch_num}_{file_name}.csv >>')
                return
        
        ## Copying DataFrame to S3 bucket
        if df.empty:
            logging.info('!!! No results in DataFrame from MSSQL !!!')
        else:
            logging.info(f"MSSQL query: \n{mssql_query}")
            logging.info(f"MSSQL results as Pandas df: {df.shape} \n{df.head(2)}")
            
            # Convert chunk to CSV bytes
            df_byte = df.to_csv(
                header=False,  # Exclude the column headers from the CSV
                index=False,  # Exclude the row index from the CSV
                sep=f'{FIELD_DELIMITER}',  # Use the pipe symbol as the column separator
                lineterminator=f'{RECORD_DELIMITER}',
                na_rep='NULL',  # Replace missing values with 'NULL'
                doublequote=True,  # Enable double quoting for values
                quotechar='"',
            ).encode()
            
            # Load chunk bytes to S3
            get_s3_hook().load_bytes(
                bytes_data=df_byte,
                bucket_name=s3_bucket_name,
                key=f"inbound/{s3_dir_path}/{file_name}.csv",
                replace=True,
            )
            
            logging.info(f'<< Uploading DataFrame to S3: {file_name}.csv >>')

    @task
    def copy_s3_to_snowflake(runtime_params):
        """
        Copies data from the S3 bucket to the Snowflake database table specified in the runtime parameters.

        The SQL query is constructed dynamically based on the data and runtime parameters.
        The data is copied using the "COPY INTO" Snowflake SQL command.
        """
        table_name = runtime_params.get("table_name")
        columns = runtime_params.get("columns")
        previous_day = runtime_params.get("aod")
        s3_dir_path = runtime_params.get("s3_dir_path")
        file_name = runtime_params.get("file_name")

        sql_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
        FROM ARES.ETL.CREO_DDL
        WHERE TABLE_NAME = '{table_name}'
        """
        ddl = get_sf_hook().get_pandas_df(sql_query)
        column_list = ddl['COLUMN_NAME'].tolist()
        data_types = ddl['DATA_TYPE'].tolist()
        null_data = ddl['NULLABLE'].tolist()

        formatted_columns = []
        for index, column_name in enumerate(column_list):
            data_type = data_types[index]
            null = null_data[index]

            cast = re.sub(r'[^a-zA-Z_]', '', data_type).lower() # remove any characters that are not a letter or underscore, and turns to lowercase
            comment = f"\t-- ${index+1}: {column_name} {data_type} {null}" # comment to add at end of line
            
            formatted_col = f"(${index+1})::{cast}" if cast != 'timestamp_ltz' else f"to_timestamp_ntz(${index+1})" # cast the column to the correct data type
            formatted_col += f', {comment}' if (index+1 < len(column_list)) else f' {comment}' # add comment with a preceeding comma, except if its the last column, then don't add a comma
            formatted_columns.append(formatted_col) # Append the single formatted column to the list
        columns = '\n'.join(formatted_columns)  # Join multiple formatted columns in the list with a newline and 2 tabs for formatting

        sql_query = f"""
            -- \\ CREO_{table_name}_HIST
            COPY INTO STG.CREO_{table_name}_HIST 
            FROM (
                SELECT 
                    METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{previous_day}'), 
                    {columns} 
                FROM @ETL.INBOUND/{s3_dir_path}/
            )
            FILE_FORMAT = ( 
                {FILE_FORMAT}
            ) 
            PATTERN = '.*{file_name}.csv.*'
        """

        get_sf_hook().run(sql_query)
        logging.info(f'<< Loading staged data to Snowflake >> \n{sql_query}')


    ## Write to the dataset file to trigger the DQ dag
    @task(outlets=[dataset_obj])
    def write_to_dataset():
        dataset_path = Path(dataset_file)
        dataset_path.touch(exist_ok=True)
        with open(dataset_file, "a") as file:
            file.write("Staging_CREO data load complete")
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    start = DummyOperator(task_id="start")
    mid = DummyOperator(task_id="mid")
    end = DummyOperator(task_id="end", trigger_rule="all_done")
    
    # for table_name in TABLE_LIST: 
    for table_name in fullTableList.keys(): 
        with TaskGroup(group_id=f"CREO_{table_name}_HIST_Task") as staging_pipeline:
    
            runtime_params = get_runtime_params(table_name, ds=AS_OF_DATE)
            mssql_to_s3_task = mssql_to_s3(runtime_params)
            copy_s3_to_snowflake_task = copy_s3_to_snowflake(runtime_params)
            
            runtime_params >> mssql_to_s3_task >> copy_s3_to_snowflake_task
            
        start >> staging_pipeline >> mid
    
    mid >> write_to_dataset() >> end

staging_dag = Staging_DAG()