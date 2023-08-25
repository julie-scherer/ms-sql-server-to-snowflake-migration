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
CSV_FILE_NAME = "LD_UTILS.csv"


# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="awsmssql_canldsql_winchkcanld_conn")
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
def get_primary_key(mssql_table_name, mssql_database):
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_CATALOG = '{mssql_database}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME COLLATE Latin1_General_CI_AI LIKE '{mssql_table_name}';
    """
    logging.info(f"Query to get primary key column: \n{query}")

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
    "Staging_LD_Utils",
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
        
        ddl_table = get_sf_hook().get_pandas_df(f"SELECT * FROM ETL.LD_DDL")
        logging.info(f"DDL Table: \n{ddl_table.head()}")

        if ddl_table.empty:
            raise ValueError(f"Dataframe is empty")
        
        # Extract the 'Table_Name' column to get a list of all the table names in the DB
        table_names = set(ddl_table['TABLE_NAME'])
        logging.info(f"Table names found: {table_names}")
        
        # Loop through each table in table_names
        for table_name in table_names:
            logging.info(f"Running {table_name}")
            # Create an empty dictionary to store the `sql`, `key_column`, `keys`, and `table_filtered_by` key-value pairs for each table
            table_dict = database_dict.get(table_name, {})

            ## 1: Custom SQL file
            table_dict["sql"] = None # assume there are no custom sql files and set to None for now (might be changed later)
            custom_sql_dir = "/usr/local/airflow/include/sql/Staging_LD"
            custom_sql_file_name = f"COPY_LD_{table_name.upper()}.sql"
            custom_sql_path = os.path.join(custom_sql_dir, custom_sql_file_name)
            if os.path.exists(custom_sql_path):
                table_dict["sql"] = custom_sql_file_name
                logging.info(f"Custom SQL file found: {custom_sql_file_name}")

            # ~ To collect the following data, we need to get the column info for the specific table in the MSSQL db
            columns = [val.strip('"') for val in ddl_table[ddl_table['TABLE_NAME'] == table_name]['COLUMN_NAME'].values]
            logging.info(f"Columns found: {columns}")
        
            ## 2: Primary key column
            engine = get_mssql_hook().get_sqlalchemy_engine()
            mssql_database = engine.url.database
            logging.info(f"Table catalog: {mssql_database}")
            
            mssql_table_name = table_name.replace(f'LD_', '').replace('_HIST', '')
            logging.info(f"Truncated table name for MSSQL query: {mssql_table_name}")
            
            key_column = get_primary_key(mssql_table_name, mssql_database) # get key column using mssql query
            table_dict["key_column"] = key_column
            logging.info(f"Primary key column: {key_column}")

            ## 3: Keys
            keys_list = [col for col in columns if '_KEY' in col]
            keys = f"{set(keys_list)}" if keys_list else None
            table_dict["keys"] = keys

            # # If there's no primary key found but there's a key in the key list, use that for key column
            # if not key_column and keys_list:
            #     table_dict["key_column"] = keys_list[0]
            #     logging.info(f"Found key column in keys list: \n{keys_list[0]}")
            
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
            SNOWFLAKE_TABLE = f"LD_{table_name.upper()}_HIST"
            database_dict[SNOWFLAKE_TABLE] = table_dict
        
        if len(table_names) != len(database_dict):
            logging.error("[ERROR] - Dictionary missing tables")
        else:
            logging.info("[SUCCESS] - All tables added")
        
        logging.info(f"Database dictionary: {database_dict}")
        return database_dict

    @task
    def create_snowflake_table():
        # get_sf_hook().run(f"DROP TABLE IF EXISTS ETL.LD_UTILS")
        
        # Create Snowflake table
        query = f"""
        CREATE OR REPLACE TABLE ETL.LD_UTILS (
            table_name VARCHAR NOT NULL
            ,sql VARCHAR
            ,key_column VARCHAR
            ,keys TEXT
            ,table_filtered_by VARCHAR
        )
        """
        get_sf_hook().run(query)
        logging.info(f"Create Snowflake table: \n{query}")

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

        s3_path=f"inbound/LD/UTILS/{CSV_FILE_NAME}"
        get_s3_hook().load_bytes(
            bytes_data=df_byte,
            bucket_name=S3_BUCKET_NAME,
            key=s3_path,
            replace=True,
        )
        logging.info(f"Successfully loaded CSV to S3: {s3_path}")
    
    @task
    def s3_to_snowflake():
        get_sf_hook().run(f"TRUNCATE TABLE IF EXISTS ETL.LD_UTILS")
        sql_query = f"""
            COPY INTO ETL.LD_UTILS
            FROM (
                SELECT $1, $2, $3, $4, $5
                FROM @ETL.INBOUND/LD/UTILS/
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