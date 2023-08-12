
import logging
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions

# - - - - - - - - - SETTINGS - - - - - - - - -

author = 'Julie Scherer'

## Global variables 
SNOWFLAKE_DATABASE = 'ARES'
SNOWFLAKE_SCHEMA = 'ETL'
MSSQL_DATABASE = 'CREO'

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_CONN = "awsmssql_creosql_creo_conn"
    # MSSQL_CONN = mssql_connection_params["conn_id"]
    MSSQL_HOOK = MsSqlHook(mssql_conn_id=MSSQL_CONN)
    return MSSQL_HOOK

def get_sf_hook():
	"""
	Returns a Snowflake hook for interacting with Snowflake.
	"""
	SF_CONN = "snowflake_default"
	SF_HOOK = SnowflakeHook(snowflake_conn_id=SF_CONN)
	return SF_HOOK


# - - - - - - SUPPORTING FUNCTIONS - - - - - - -

# Run MSSQL query to get Primary Key Column
def get_primary_key(mssql_table_name):
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_CATALOG = '{MSSQL_DATABASE}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{mssql_table_name}';
    """
    df = get_mssql_hook().get_pandas_df(query)
    if df.empty:
        return None
    primary_key = df.iloc[0,0]
    return primary_key


# - - - - - - - - - - 

default_args = {
    'owner': author,
    'start_date': datetime(2023, 7, 31),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'Staging_Utils',
    start_date=pendulum.datetime(2023, 7, 1, tz='US/Eastern'),
    catchup=False,
    schedule_interval='@once',
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def Staging_Utils():
    
    @task
    def get_staging_utils():
        # Create an empty dictionary for the database, which will store all the tables' dictionaries
        database_dict = {}
        
        ddl_table = get_sf_hook().get_pandas_df(f"SELECT Table_Name, Column_Data FROM ETL.{MSSQL_DATABASE}_DDL")
        
        # Extract the 'Table_Name' column to get a list of all the table names in the DB
        table_names = ddl_table['TABLE_NAME'].tolist() 
        
        # Loop through each table in table_names
        for tbl in table_names:
            # Create an empty dictionary to store the `sql`, `key_column`, `keys`, and `table_filtered_by` key-value pairs for each table
            table_dict = database_dict.get(tbl, {})

            ## 1: Custom SQL file
            table_dict["sql"] = None # assume there are no custom sql files and set to None for now (might be changed later)
            
            ## 2: Primary column
            key_column = get_primary_key(tbl) # get key column using mssql query
            table_dict["key_column"] = key_column

            # For the next 2 keys, we need to get the 'Column_Data' for the specific table in the MSSQL db
            col_data = ddl_table[ddl_table['TABLE_NAME'] == tbl]['COLUMN_DATA'].values[0]

            # Next, we need to format the raw output so we can extract the keys and date columns
            schema = col_data.replace('"en-ci"', "").replace("NOT NULL", "").replace("NULL", "").replace("'", "").replace('"', '').replace("\n", "").replace(',', "").strip()
            
            ## 3: Keys
            keys = [word for word in schema.split() if '_KEY' in word]
            table_dict["keys"] = f'{set(keys)}'
            
            ## 4: Filtered by column
            table_dict["table_filtered_by"] = None
            if 'DATE_ENTERED' in schema:
                table_dict["table_filtered_by"] = "DATE_ENTERED"
            elif 'ENTERED_AT' in schema:
                table_dict["table_filtered_by"] = "ENTERED_AT"
            elif 'DATE_SENT' in schema:
                table_dict["table_filtered_by"] = "DATE_SENT"
            elif 'DATE_COMPLETED' in schema:
                table_dict["table_filtered_by"] = "DATE_COMPLETED"
            elif 'DATE_STARTED' in schema:
                table_dict["table_filtered_by"] = "DATE_STARTED"
            elif 'DATE_ENDED' in schema:
                table_dict["table_filtered_by"] = "DATE_ENDED"
            elif 'DATE_UPDATED' in schema:
                table_dict["table_filtered_by"] = "DATE_UPDATED"
            elif 'DATE_START' in schema:
                table_dict["table_filtered_by"] = "DATE_START"
            elif 'DATE_END' in schema:
                table_dict["table_filtered_by"] = "DATE_END"
            else:
                table_dict["table_filtered_by"] = None

            logging.info(f"Table dictionary: {table_dict}")

            # Add the Snowflake table name as a key to the database dict
            # and assign the table dict as the value of that key
            SNOWFLAKE_TABLE = f"{MSSQL_DATABASE}_{tbl.upper()}_HIST"
            database_dict[SNOWFLAKE_TABLE] = table_dict
        
        if len(table_names) != len(database_dict):
            logging.error("[ERROR] - Dictionary missing tables")
        else:
            logging.info("[SUCCESS] - All tables added")
        
        logging.info(f"Database dictionary: {database_dict}")
        return database_dict

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule="all_done")
    get_staging_utils_task = get_staging_utils()
    
    start >> get_staging_utils_task >> end

staging_utils_dag = Staging_Utils()