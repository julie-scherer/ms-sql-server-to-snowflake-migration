from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

# Import utils dictionaries
from Staging_CM import fullTableList as cm_utils
from Staging_LD import fullTableList as ld_utils

sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
s3_bucket = Variable.get("s3_etldata_bucket_var")

# - - - - - - - - - - 

# Create Snowflake table
def create_snowflake_table(database_name):
    table_name = f"{database_name.upper()}_UTILS" ## CM_UTILS, LD_UTILS, etc
    query = f"""
    CREATE OR REPLACE TABLE ETL.{table_name} (
        table_name VARCHAR NOT NULL
        ,sql VARCHAR
        ,table_filtered_by BOOLEAN
        ,key_column VARCHAR
    )
    """
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
    return df


# Load the DataFrame into Snowflake table
def load_snowflake_table(df, table_name):
    # Convert DataFrame to CSV and upload it to Snowflake's internal stage
    csv_file_path = f"{table_name.lower()}_data.csv"
    df.to_csv(csv_file_path, index=False)

    snowflake_conn = sf_hook.get_conn()

    # Upload the CSV file to the Snowflake internal stage
    snowflake_conn.cursor().execute(f"PUT 'file://{csv_file_path}' @~")

    # Copy data from the stage to the Snowflake table
    copy_sql = f"""
        COPY INTO ETL.{table_name} 
        FROM @~/{csv_file_path}
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
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

utils = {
    'CM': cm_utils,
    'LD': ld_utils,
}

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
