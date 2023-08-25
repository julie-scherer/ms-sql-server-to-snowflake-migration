import os
import logging
import re
from datetime import datetime, timedelta
# import connectorx as cx  ##//connectorx==0.3.2a2
import pendulum
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

from astro import sql as aql

from include.functions import ms_teams_callback_functions
import include.sql.backfill_sql_statements as sql_stmts

datestamp = '2023-07-27'

## Global variables 
# >> Defined in upper case to distinguish from locally defined variables
DATABASE_NAME = 'CREO'
TABLE_NAMES = ['Contact']
FILE_FORMAT = \
"""TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 0
"""
PATTERN = 'Backfill_[0-9]+\.csv\.gz'

START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')

MSSQL_CONN = "awsmssql_creosql_creo_conn"
MSSQL_HOOK = MsSqlHook(mssql_conn_id=MSSQL_CONN)

SF_CONN = "snowflake_default"
SF_HOOK = SnowflakeHook(snowflake_conn_id=SF_CONN)

S3_CONN = "aws_s3_conn"
S3_BUCKET = "s3_etldata_bucket_var" 
# S3_BUCKET = Variable.get("s3_etldata_bucket_var") #! use in prod, comment out in local testing
S3_HOOK = S3Hook(aws_conn_id=S3_CONN)

# - - - - - - - - - - - - - - - - - - - -

# Default DAG args
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
    "on_failure_callback": ms_teams_callback_functions.failure_callback
}

# Initialize the DAG
@dag(
    "Backfill_DAG",
    start_date=START_DATE,
    catchup=False,
    schedule_interval='0 6 * * *',  # 6AM EST
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args
)
def backfill_dag():
    start = DummyOperator(task_id="start") 
    end = DummyOperator(task_id="end", trigger_rule="all_done")
    aod = datetime.strptime(datestamp, "%Y-%m-%d") #// datestamp input in YYYY-MM-DD format
    
    ## Get MSSQL table definitions by running SQL query in SQL statements
    @task
    def get_mssql_table_definitions(database_name, table_name):
        df = MSSQL_HOOK.get_pandas_df(sql_stmts.get_table_definitions)
        print(df)

        ddl_df = pd.read_csv(f'include/{database_name.upper()}.csv')

        table_row = ddl_df.loc[ddl_df['TableName'] == table_name]
        table_definitions = table_row['ColumnData'].iloc[0]

        logging.info(f"Successfully retrieved table definitions for {database_name} {table_name}.")
        return table_definitions

    ## Transform table definitions from MSSQL to be used in Snowflake
    @task
    def transform_mssql_ddl(table_definitions):
        schema = re.sub(r'(?<![0-9]),', ',\n\t', table_definitions).replace('"en-ci"',"'en-ci'").replace(' ,',',')
        return schema

    ## Format columns for COPY INTO query
    @task
    def format_cols_for_copy_into(table_definitions):
        columns = re.split(r'(?<![0-9]),', table_definitions)
        formatted_columns = []
        for index, column in enumerate(columns, start=1):
            column_data = column.strip().split(' ') # remove any trailing white spaces on the left or right ends of the string, and then split the string into a list
            col_name, col_type = column_data[0], column_data[1]

            cast = re.sub(r'[^a-zA-Z_]', '', col_type).lower() # remove any characters that are not a letter or underscore, and turns to lowercase
            cmt = f"\t-- ${index}: {col_name} {col_type} {'NOT NULL' if 'NOT' in column_data else 'NULL'}" # comment to add at end of line
            
            formatted_col = f"(${index})::{cast}" if cast != 'timestamp_ltz' else f"to_timestamp_ntz(${index})" # cast the column to the correct data type
            formatted_col += f', {cmt}' if (index < len(columns)) else f' {cmt}' # add comment with a preceeding comma, except if its the last column, then don't add a comma

            formatted_columns.append(formatted_col) # Append the single formatted column to the list

        return '\n\t\t'.join(formatted_columns)  # Join multiple formatted columns in the list with a newline and 2 tabs for formatting

    ## Create Snowflake table
    @task
    def create_snowflake_table(database_name, table_name, schema):
        return SnowflakeOperator(
            task_id=f"create_{database_name.lower()}_{table_name.lower()}_table",
            sql=sql_stmts.create_snowflake_table,
            params={
                "database_name": database_name.upper(),
                "table_name": table_name.upper(),
                "schema": schema,
                }
            )

    def get_primary_key_columns(database_name, table_name):
        # Execute the query and get the primary key column(s)
        result = MSSQL_HOOK.get_records(sql_stmts.get_primary_key_columns, parameters={"database_name": database_name, "table_name": table_name})
        primary_key_columns = [row[0] for row in result]  # Fetch all rows and extract the first column
        order_by = ', '.join(primary_key_columns) # Build the ORDER BY clause based on the primary key column(s)
        return order_by # Return the primary key column(s) as a list of column names

    ## Load CSVs to S3
    @task
    def csv_to_s3(database_name, table_name, batch_size=1000000):
        s3_path = f"inbound/{database_name}/Backfill/{table_name}"
        offset = 0
        batch_number = 1

        order_by = get_primary_key_columns(database_name, table_name)
        while True:
            # Read data from MSSQL in batches
            query = f"SELECT * FROM {table_name} ORDER BY {order_by} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            df = MSSQL_HOOK.get_pandas_df(query)
            logging.info(f"Running {query}: \n{df.head()}")

            # If there is no more data to read, break the loop
            if df.empty:
                logging.info(f"No data to read, breaking from export csv and load to s3 task")
                break

            # Convert the batch DataFrame to CSV bytes
            df_byte = df.to_csv(
                header=False,  # Exclude the column headers from the CSV
                index=False,  # Exclude the row index from the CSV
                sep="|",  # Use the pipe symbol as the column separator
                na_rep='NULL',  # Replace missing values with 'NULL'
                compression='gzip',  # Compress the CSV file using gzip
                doublequote=True,  # Enable double quoting for values
                quotechar='"'
            ).encode()
            logging.info(f"Successfully converted dataframe to CSV")

            # Load the CSV bytes to S3
            s3_file = f"{table_name}_Backfill_{batch_number}.csv.gz"
            S3_HOOK.load_bytes(
                bytes_data=df_byte,
                bucket_name=S3_BUCKET,
                replace=True,
                key=f"{s3_path}/{s3_file}"
            )
            logging.info(f"Successfully loaded CSV to S3: {s3_file}")

            # Update offset and batch number for the next batch
            offset += batch_size
            batch_number += 1

            logging.info(f"Successfully loaded {database_name} {table_name} batch {batch_number} ({offset - batch_size} - {offset}) to {s3_path}/{s3_file}")

    ## Copy staged data into Snowflake
    @task
    def s3_to_snowflake(database_name, table_name, aod, columns):
        file_format = FILE_FORMAT
        pattern = PATTERN
        return SnowflakeOperator(
            task_id=f"load_staged_data_{database_name.lower()}_{table_name.lower()}",
            sql=sql_stmts.load_staged_data,
            params={
                "database_name": database_name,
                "table_name": table_name,
                "columns": columns,
                "aod": aod,
                "file_format": file_format,
                "pattern": pattern,
            }
        )

    # - - - - - - - - - - - - - - - - - - - -
    # Initialize an empty list to hold the tasks
    backfill_task_group = []
    database_name = DATABASE_NAME
    table_names = TABLE_NAMES
    for idx, table_name in enumerate(table_names):
        table_definitions = get_mssql_table_definitions(database_name, table_name)

        schema = transform_mssql_ddl(table_definitions)
        create_snowflake_table_task = create_snowflake_table(database_name, table_name, schema)
        
        csv_to_s3_task = csv_to_s3(database_name, table_name)

        columns = format_cols_for_copy_into(table_definitions)
        s3_to_snowflake_task = s3_to_snowflake(database_name, table_name, aod, columns)

        # Define the dependencies within the TaskGroup
        create_snowflake_table_task >> csv_to_s3_task >> s3_to_snowflake_task

        # Append the TaskGroup to the backfill_task_group list
        backfill_task_group.append(create_snowflake_table_task)
        backfill_task_group.append(csv_to_s3_task)
        backfill_task_group.append(s3_to_snowflake_task)

    # Define the task dependencies outside the TaskGroup
    start >> backfill_task_group >> end

# Instantiate the DAG
backfill_dag = backfill_dag()