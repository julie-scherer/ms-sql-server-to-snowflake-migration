
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
from airflow.operators.python_operator import BranchPythonOperator

from include.functions import ms_teams_callback_functions

import math

# - - - - - - - - - SETTINGS - - - - - - - - -

# creo_mssql_hook = MsSqlHook(mssql_conn_id="awsmssql_creosql_creo_conn")
# creoarchive_mssql_hook = MsSqlHook(mssql_conn_id="awsmssql_creosql_creoarchive_conn")
creoarchive2_mssql_hook = MsSqlHook(mssql_conn_id="awsmssql_creosql_creoarchive2_conn")

s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

# Default DAG args
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
}

# Initialize the DAG
@dag(
    "MessageDeliveryStatus",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    catchup=False,
    schedule_interval="@once",
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def BackfillTable():

    @task
    def message_delivery_status():
        # dbName = 'CREOArchive'
        # totalRows = 73922555
        # chunkSize = 1000000
        # # --> 74 csvs
        # start = 66

        dbName = 'CREOArchive2'
        totalRows = 673708425   # 673,708,425 
        chunkSize = 1000000
        # --> 673 CSVs
        start = 181 

        totalIterations = math.ceil(totalRows / chunkSize) 
        for i in range(start,totalIterations):
            chunk_idx = i+1
            offset = i*chunkSize

            mssql_query = f"""SET NOCOUNT ON ; 
            SELECT
                MESSAGE_DELIVERY_STATUS_KEY
                ,MESSAGE_KEY
                ,DELIVERY_STATUS_KEY
                ,DATE_ENTERED
                ,Replace(Replace(DETAIL,CHAR(10),''),CHAR(13),'') AS DETAIL
            FROM {dbName}.[dbo].[MessageDeliveryStatus]
            ORDER BY MESSAGE_DELIVERY_STATUS_KEY
            OFFSET {offset} ROWS FETCH 
            NEXT {chunkSize} ROWS ONLY;""" 
            logging.info(f"MSSQL query: \n{mssql_query}")
            
            ## Getting the MSSQL data
            df = creoarchive2_mssql_hook.get_pandas_df(mssql_query)
            logging.info(f"Pandas dataframe: \n{df.head}")

            # Convert chunk to CSV bytes
            df_byte = df.to_csv(
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
                bucket_name=Variable.get("s3_etldata_bucket_var"),
                key=f"inbound/{dbName}/MessageDeliveryStatus/MessageDeliveryStatus_{chunk_idx}.csv",
                replace=True,
            )
            logging.info(f'Uploaded {chunk_idx} chunk to S3: inbound/{dbName}/MessageDeliveryStatus/MessageDeliveryStatus_{chunk_idx}.csv >>')
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule="all_done")
    message_delivery_status_task = message_delivery_status()
    
    start >> message_delivery_status_task >> end

backfill = BackfillTable()
