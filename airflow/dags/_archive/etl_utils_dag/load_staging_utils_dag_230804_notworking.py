import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator

# Import utils dictionaries
from Staging_CM import fullTableList as cm_utils
from Staging_LD import fullTableList as ld_utils
from Staging_CREO import fullTableList as creo_utils

utils = {
    # 'CM': cm_utils,
    # 'LD': ld_utils,
    'CREO': creo_utils,
}

sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

# - - - - - - - - - - 

# Create Snowflake table
def create_snowflake_table(database_name):
    table_name = f"{database_name.upper()}_UTILS" ## CM_UTILS, LD_UTILS, etc
    query = f"""
    CREATE OR REPLACE TABLE ARES.ETL.{table_name} (
        table_name VARCHAR NOT NULL
        ,sql VARCHAR
        ,table_filtered_by VARCHAR -- use VARCHAR for CREO dag and BOOLEAN for CM & LD dags
        ,key_column TEXT
    )
    """
    logging.info(f"Snowflake DDL Query: \n{query}")
    sf_hook.run(query)
    return table_name


# Read the imported fullTableList dictionaries in as a Pandas df
def read_table_list_dict(util_data):
    df = pd.DataFrame\
        .from_dict(
            util_data, 
            orient='index'
        )\
        .reset_index(level=0)\
        .rename(columns={'index':'table_name'})
    logging.info(f"Pandas Dataframe: \n{df.head()}")
    return df


# Load the DataFrame into Snowflake table
def load_snowflake_table(df, table_name):
    # Convert DataFrame to CSV and upload it to Snowflake's internal stage
    csv_dir = "include"
    csv_file = f"{table_name.lower()}_data.csv"
    df.to_csv(
        os.path.join(csv_dir, csv_file), 
        na_rep='NULL', 
        index=False,
        sep="|",
    )

    sf_hook.run(f"TRUNCATE TABLE IF EXISTS ARES.ETL.{table_name};")

    snowflake_conn = sf_hook.get_conn()

    # Upload the CSV file to the Snowflake internal stage
    snowflake_conn.cursor().execute(f"PUT 'file://{csv_dir}/{csv_file}' @~/{csv_dir}")

    # Copy data from the stage to the Snowflake table
    copy_sql = f"""
        COPY INTO ETL.{table_name} 
        FROM @~/{csv_dir}/{csv_file}
        FILE_FORMAT = (
            TYPE = 'CSV'
            COMPRESSION = NONE
            SKIP_HEADER = 1
            FIELD_DELIMITER = '|'
        )
    """
    snowflake_conn.cursor().execute(copy_sql)



# Load the data into Snowflake
def load_utils_to_snowflake_task(database_name, util_data):
    df = read_table_list_dict(util_data)
    table_name = create_snowflake_table(database_name)
    load_snowflake_table(df, table_name)

# - - - - - - - - - - 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 31),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_utils_to_snowflake',
    default_args=default_args,
    schedule_interval=None, 
)


# Create individual tasks for each item in the 'utils' dictionary
for key, val in utils.items():
    task_id = f'load_utils_to_snowflake_{key.lower()}_task'
    load_task = PythonOperator(
        task_id=task_id,
        python_callable=load_utils_to_snowflake_task,
        op_args=[key, val],
        dag=dag,
    )
    load_task
