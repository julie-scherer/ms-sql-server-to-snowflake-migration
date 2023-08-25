# git checkout -b AB#203810-Generate-DDL-DAG

import os
import logging
import re
from datetime import datetime, timedelta
import pendulum
import pandas as pd

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from include.functions import ms_teams_callback_functions

## Global variables 

author = 'Julie Scherer'

SNOWFLAKE_DATABASE = 'ARES'
SNOWFLAKE_SCHEMA = 'ETL'
MSSQL_DATABASE = 'CREO'

FILE_FORMAT = """
    TYPE = CSV
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 1
    NULL_IF = 'NULL'
"""

S3_BUCKET_NAME = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001

START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')

SF_CONN_ID = "snowflake_default"
S3_CONN_ID = "aws_s3_conn"

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
                WHEN t.name = 'varbinary' THEN 'VARCHAR'
                WHEN t.name = 'binary' THEN 'VARCHAR'
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

# - - - - - - - - - - - - - - - - - - - -

# Default DAG args
default_args = {
    'owner': author,
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
            CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{MSSQL_DATABASE}_DDL (
                Table_Name VARCHAR(128),
                Column_Data VARCHAR
            );
        """
        get_sf_hook().run(create_ddl_table)

    @task
    def mssql_to_s3():
        df = get_mssql_hook().get_pandas_df(get_table_definitions)
        logging.info(f"df = {df}")

        out_dir=f"include/DDL/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}"
        os.makedirs(out_dir, exist_ok=True)
        csv_file=f"{MSSQL_DATABASE}_DDL.csv"
        file_path = os.path.join(out_dir, csv_file)

        df.to_csv(
            file_path,
            header=True,  # Data file needs to have a header row
            index=False,  # Exclude the row index from the CSV
            sep="|",  # Use the pipe symbol as the column separator
            na_rep='NULL',  # Replace missing values with 'NULL'
            doublequote=True,  # Enable double quoting for values
        )

        # Load zip file to S3
        # https://airflow.apache.org/docs/apache-airflow/1.10.4/_api/airflow/hooks/S3_hook/index.html#airflow.hooks.S3_hook.S3Hook.load_file
        get_s3_hook().load_file(
            filename=file_path,
            key=f"inbound/{MSSQL_DATABASE}/DDL/{MSSQL_DATABASE}_DDL.csv",
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )

        os.remove(file_path)
        os.removedirs(out_dir)

    @task
    def s3_to_snowflake():
        get_sf_hook().run(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{MSSQL_DATABASE}_DDL")
        sql_query = f"""
			COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{MSSQL_DATABASE}_DDL
			FROM (
				SELECT $1, $2 
				FROM @ETL.INBOUND/{MSSQL_DATABASE}/DDL/
			)
			FILE_FORMAT = ( 
				{FILE_FORMAT}
			) 
			PATTERN = '.*{MSSQL_DATABASE}_DDL.csv'    
		"""
        get_sf_hook().run(sql_query)
	
    create_snowflake_table_task = create_snowflake_table()
    mssql_to_s3_task = mssql_to_s3()
    s3_to_snowflake_task = s3_to_snowflake()

    create_snowflake_table_task >> mssql_to_s3_task >> s3_to_snowflake_task


generate_ddl_dag = Generate_DDL_DAG()
