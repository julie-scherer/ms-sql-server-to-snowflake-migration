import os
import logging
from datetime import datetime, timedelta
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable

from include.functions import ms_teams_callback_functions

# - - - - - - - - - SETTINGS - - - - - - - - -

author = 'Julie Scherer'

S3_BUCKET_NAME = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001
FILE_FORMAT = """
	TYPE = CSV
	FIELD_DELIMITER = '|'
	RECORD_DELIMITER = '\\n'
	SKIP_HEADER = 1
	NULL_IF = 'NULL'
"""

TABLE_NAME = "ARES.ETL.VC"
LOCAL_OUT_DIR = "include/UTILS/ARES/ETL"
CSV_FILE_NAME = "VC_UTILS.csv"
S3_PATH = "VC/UTILS"



# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="mssql_ictvcsql_winchkictvc_conn")
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

# - - - - - - SUPPORTING FUNCTIONS - - - - - - -

# Run MSSQL query to get Primary Key Column
def get_primary_key(mssql_table_name):
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_CATALOG = 'VC' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{mssql_table_name}';
    """
    df = get_mssql_hook().get_pandas_df(query)
    if df.empty:
        return None
    primary_key = df.iloc[0,0]
    return primary_key

# - - - - - - - - - - 

default_args = {
    'owner': author,
    'start_date': pendulum.datetime(2023, 7, 1, tz='US/Eastern'),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    "Staging_VC_Utils",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    catchup=False,
    schedule_interval="@once",
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def Staging_Utils():
    
    @task(multiple_outputs=True)
    def get_staging_utils() -> dict:
        # Create an empty dictionary for the database, which will store all the tables' dictionaries
        database_dict = {}
        
        ddl_table = get_sf_hook().get_pandas_df(f"SELECT * FROM {TABLE_NAME}_DDL")
        logging.info(f"DDL Table: \n{ddl_table.head()}")

        if ddl_table.empty:
            raise ValueError(f"Dataframe is empty")
        
        # Extract the 'Table_Name' column to get a list of all the table names in the DB
        table_names = set(ddl_table['TABLE_NAME'])
        logging.info(f"Table names found: \n{table_names}")
        
        # Loop through each table in table_names
        for tbl in table_names:
            logging.info(f"Running {tbl}")
            # Create an empty dictionary to store the `sql`, `key_column`, `keys`, and `table_filtered_by` key-value pairs for each table
            table_dict = database_dict.get(tbl, {})

            ## 1: Custom SQL file
            table_dict["sql"] = None # assume there are no custom sql files and set to None for now (might be changed later)
            
            ## 2: Primary column
            key_column = get_primary_key(tbl) # get key column using mssql query
            table_dict["key_column"] = key_column

            # For the next 2 keys, we need to get the column info for the specific table in the MSSQL db
            columns = [val.strip('"') for val in ddl_table[ddl_table['TABLE_NAME'] == tbl]['COLUMN_NAME'].values]
            logging.info(f"Columns found: \n{columns}")
        
            ## 3: Keys
            keys_list = [col for col in columns if '_KEY' in col]
            keys = f"{set(keys_list)}" if keys_list else None
            table_dict["keys"] = keys
            
            ## 4: Filtered by column
            if 'DATE_ENTERED' in columns:
                table_dict["table_filtered_by"] = "DATE_ENTERED"
            elif 'ENTERED_AT' in columns:
                table_dict["table_filtered_by"] = "ENTERED_AT"
            elif 'DATE_SENT' in columns:
                table_dict["table_filtered_by"] = "DATE_SENT"
            elif 'DATE_COMPLETED' in columns:
                table_dict["table_filtered_by"] = "DATE_COMPLETED"
            elif 'DATE_STARTED' in columns:
                table_dict["table_filtered_by"] = "DATE_STARTED"
            elif 'DATE_ENDED' in columns:
                table_dict["table_filtered_by"] = "DATE_ENDED"
            elif 'DATE_UPDATED' in columns:
                table_dict["table_filtered_by"] = "DATE_UPDATED"
            elif 'DATE_START' in columns:
                table_dict["table_filtered_by"] = "DATE_START"
            elif 'DATE_END' in columns:
                table_dict["table_filtered_by"] = "DATE_END"
            else:
                table_dict["table_filtered_by"] = None

            logging.info(f"Table dictionary: {table_dict}")

            # Add the Snowflake table name as a key to the database dict
            # and assign the table dict as the value of that key
            SNOWFLAKE_TABLE = f"VC_{tbl.upper()}_HIST"
            database_dict[SNOWFLAKE_TABLE] = table_dict
        
        if len(table_names) != len(database_dict):
            logging.error("[ERROR] - Dictionary missing tables")
        else:
            logging.info("[SUCCESS] - All tables added")
        
        logging.info(f"Database dictionary: {database_dict}")
        return database_dict

    @task
    def create_snowflake_table():
        # get_sf_hook().run(f"DROP TABLE IF EXISTS {TABLE_NAME}_UTILS")
        
        # Create Snowflake table
        query = f"""
        CREATE OR REPLACE TABLE {TABLE_NAME}_UTILS (
            table_name VARCHAR NOT NULL
            ,sql VARCHAR
            ,key_column VARCHAR
            ,keys TEXT
            ,table_filtered_by VARCHAR -- use VARCHAR for CREO dag and BOOLEAN for CM & LD dags
        )
        """
        get_sf_hook().run(query)
        logging.info(f"Create Snowflake table: \n{query}")
        return TABLE_NAME

    @task
    def df_to_s3(fullTableList):
        # Read the fullTableList dictionary in as a Pandas df
        df = pd.DataFrame\
            .from_dict(
                fullTableList, 
                orient='index'
            )\
            .reset_index(level=0)\
            .rename(columns={'index':'table_name'})
        
        if df.empty:
            raise ValueError(f"Dataframe is empty")
        logging.info(f"Successfully read in table list: \n{df.head()}")

        df_byte = df.to_csv(
            header=True,  # Data file needs to have a header row if using aql
            index=False,  # Exclude the row index from the CSV
            sep="|",  # Use the pipe symbol as the column separator
            na_rep='NULL',  # Replace missing values with 'NULL'
            doublequote=True,  # Enable double quoting for values
        ).encode()

        s3_path=f"inbound/{S3_PATH}/{CSV_FILE_NAME}"
        get_s3_hook().load_bytes(
            bytes_data=df_byte,
            bucket_name=S3_BUCKET_NAME,
            key=s3_path,
            replace=True,
        )
        logging.info(f"Successfully loaded CSV to S3: {s3_path}")
    
    @task
    def s3_to_snowflake():
        get_sf_hook().run(f"TRUNCATE TABLE IF EXISTS {TABLE_NAME}_UTILS")
        sql_query = f"""
            COPY INTO {TABLE_NAME}_UTILS
            FROM (
                SELECT $1, $2, $3, $4, $5
                FROM @ETL.INBOUND/{S3_PATH}/
            )
            FILE_FORMAT = ( 
                {FILE_FORMAT}
            ) 
            PATTERN = '.*{CSV_FILE_NAME}'    
        """
        
        get_sf_hook().run(sql_query)
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule="all_done")

    fullTableList = get_staging_utils()

    create_snowflake_table_task = create_snowflake_table()
    df_to_s3_task = df_to_s3(fullTableList)
    s3_to_snowflake_task = s3_to_snowflake()

    start >> create_snowflake_table_task >> df_to_s3_task >> s3_to_snowflake_task >> end

staging_etl_utils_dag = Staging_Utils()


# git checkout feature/Staging_Utils_DAG
# https://github.com/CuroFinTechCorp/Curo-Astro/blob/feature/Staging_Utils_DAG/dags/Staging_Utils_DAG.py