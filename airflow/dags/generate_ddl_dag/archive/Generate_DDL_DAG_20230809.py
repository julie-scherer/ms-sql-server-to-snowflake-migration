# git checkout -b AB#203810-Generate-DDL-DAG

import os
import logging
import re
from datetime import datetime, timedelta
import pendulum
import pandas as pd

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions

from airflow.models import Connection
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.settings import Session

from astro import sql as aql
from astro.files import File, get_file_list
from astro.sql.table import Metadata, Table
from astro.constants import FileType

from include.functions import ms_teams_callback_functions
import include.sql.backfill_sql_statements as sql_stmts

## Global variables 
SNOWFLAKE_DATABASE = 'ARES'
SNOWFLAKE_SCHEMA = 'ETL'

DATABASE_NAME = 'CREO'
TABLE_LIST = ['DatasetValue']

MSSQL_CONN = "awsmssql_creosql_creo_conn"
MSSQL_HOOK = MsSqlHook(mssql_conn_id=MSSQL_CONN)

SF_CONN = "snowflake_default"
SF_HOOK = SnowflakeHook(snowflake_conn_id=SF_CONN)

S3_BUCKET_NAME = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001

START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')

# - - - - - - - - - - - - - - - - - - - -

get_table_definitions = f"""
-- Create a temporary table to store the results
DROP TABLE IF EXISTS #TableStats;
CREATE TABLE #TableStats (
    [TableName] [VARCHAR](128),
    [ColumnData] [VARCHAR](MAX),
);

-- Cursor to iterate through each table
DECLARE @TableName NVARCHAR(128);
DECLARE @ColumnData VARCHAR (MAX);

DECLARE tableCursor CURSOR FOR
SELECT name FROM sys.tables;

OPEN tableCursor;
FETCH NEXT FROM tableCursor INTO @TableName;

-- Loop through each table
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ColumnData = (
        SELECT STRING_AGG(CONCAT(
            c.name, ' '
            ,(CASE
                WHEN t.name = 'varchar' THEN CONCAT('VARCHAR(', t.max_length, ')')
                WHEN t.name = 'nvarchar' THEN CONCAT('VARCHAR(', t.max_length, ')')
                WHEN t.name = 'tinyint' THEN 'SMALLINT'
                WHEN t.name = 'int' THEN 'INT'
                WHEN t.name = 'bigint' THEN 'BIGINT'
                WHEN t.name = 'decimal' THEN CONCAT('DECIMAL(', t.precision, ',', 0, ')')
                WHEN t.name = 'numeric' THEN 'NUMERIC'
                WHEN t.name = 'date' THEN 'DATE'
                WHEN t.name = 'time' THEN 'TIME'
                WHEN t.name = 'datetime2' THEN 'TIMESTAMP_LTZ'
                WHEN t.name = 'datetimeoffset' THEN 'TIMESTAMP_LTZ'
                WHEN t.name = 'real' THEN 'INT'
                WHEN t.name = 'money' THEN 'NUMBER(19,4)'
                WHEN t.name = 'smalldatetime' THEN 'TIMESTAMP_LTZ'
                WHEN t.name = 'float' THEN 'FLOAT'
                WHEN t.name = 'bit' THEN 'BOOLEAN'
                WHEN t.name = 'smallmoney' THEN 'NUMBER(10,4)'
                WHEN t.name = 'hierarchyid' THEN 'VARIANT'
                WHEN t.name = 'geometry' THEN 'VARIANT'
                WHEN t.name = 'geography' THEN 'VARIANT'
                WHEN t.name = 'varbinary' THEN 'VARBINARY'
                WHEN t.name = 'binary' THEN 'BINARY'
                WHEN t.name = 'char' THEN CONCAT('CHAR(', t.max_length, ')')
                WHEN t.name = 'timestamp' THEN 'TIMESTAMP'
                WHEN t.name = 'sysname' THEN CONCAT('STRING(', t.max_length, ')')
                WHEN t.name = 'uniqueidentifier' THEN 'VARCHAR(36)'
                ELSE t.name
            END),' '
            ,(CASE WHEN c.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END),' '
            ,(CASE
                WHEN c.collation_name = 'SQL_Latin1_General_CP1_CI_AS' THEN 'COLLATE "en-ci"'
                ELSE ''
            END)
        ), ',')
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(@TableName)
    );

    -- Insert into #TableStats
    INSERT INTO #TableStats (TableName, ColumnData)
    VALUES (@TableName, @ColumnData);

    FETCH NEXT FROM tableCursor INTO @TableName;
END;

CLOSE tableCursor;
DEALLOCATE tableCursor;

-- Retrieve the results
SELECT *
FROM #TableStats
ORDER BY TableName;
"""



def get_mssql_hook():
	"""
	Returns a MSSQL hook for interacting with MSSQL.
	"""
	MSSQL_CONN = "awsmssql_creosql_creo_conn"
	MSSQL_HOOK = MsSqlHook(mssql_conn_id=MSSQL_CONN)
	return MSSQL_HOOK

def get_s3_hook():
	"""
	Returns a S3 hook for interacting with S3.
	"""
	S3_CONN = "aws_s3_conn"
	S3_HOOK = S3Hook(aws_conn_id=S3_CONN)
	return S3_HOOK

def get_sf_hook():
	"""
	Returns a Snowflake hook for interacting with Snowflake.
	"""
	SF_CONN = "snowflake_default"
	SF_HOOK = SnowflakeHook(snowflake_conn_id=SF_CONN)
	return SF_HOOK


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
    "Generate_DDL_DAG",
    start_date=START_DATE,
    catchup=False,
    schedule_interval='@once',
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def Generate_DDL_DAG():

    @task
    def create_snowflake_table():
        create_ddl_table = f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_DATABASE}_DDL (
                TableName VARCHAR(128),
                ColumnData VARCHAR
            );
        """
        get_sf_hook().run(create_ddl_table)

    @task
    def mssql_to_s3():
        df = MSSQL_HOOK.get_pandas_df(get_table_definitions)
        df_byte = df.to_csv(
            header=False,  # Exclude the column headers from the CSV
            index=False,  # Exclude the row index from the CSV
            sep="|",  # Use the pipe symbol as the column separator
            na_rep='NULL',  # Replace missing values with 'NULL'
            compression='gzip',  # Compress the CSV file using gzip
            doublequote=True,  # Enable double quoting for values
            quotechar='"'
        ).encode()

        # Load zip file to S3
        get_s3_hook().load_bytes(
            bytes_data=df_byte,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
            key=f"inbound/{DATABASE_NAME}/DDL/{DATABASE_NAME}_DDL.csv.gz",
        )

    @task
    def s3_to_snowflake():
        s3_to_snowflake = aql.load_file(
            task_id="s3_to_snowflake",
            input_file=File(
                path=f"s3://{S3_BUCKET_NAME}/inbound/inbound/{DATABASE_NAME}/DDL/{DATABASE_NAME}_DDL.csv.gz", 
                filetype=FileType.CSV
            ),
            output_table=Table(
                conn_id="snowflake_default",
                metadata=Metadata(database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA),
                name=f"{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_DATABASE}_DDL",
            ),
            if_exists="replace",
        )
        aql.cleanup() 

    create_snowflake_table_task = create_snowflake_table()
    mssql_to_s3_task = mssql_to_s3()
    s3_to_snowflake_task = s3_to_snowflake()

    create_snowflake_table_task >> mssql_to_s3_task >> s3_to_snowflake_task


generate_ddl_dag = Generate_DDL_DAG()
