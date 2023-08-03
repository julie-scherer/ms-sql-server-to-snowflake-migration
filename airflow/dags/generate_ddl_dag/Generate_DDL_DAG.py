# git checkout -b AB#203810-Generate-DDL-DAG

import os
import logging
import re
from datetime import datetime, timedelta
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

from include.functions import ms_teams_callback_functions
import include.sql.backfill_sql_statements as sql_stmts

## Global variables 
SNOWFLAKE_TABLE = "ETL.CREO_DDL"

MSSQL_CONN = "awsmssql_creosql_creo_conn"
MSSQL_HOOK = MsSqlHook(mssql_conn_id=MSSQL_CONN)

SF_CONN = "snowflake_default"
SF_HOOK = SnowflakeHook(snowflake_conn_id=SF_CONN)

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

create_ddl_table = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
    TableName VARCHAR(128),
    ColumnData VARCHAR
);
"""

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
def generate_ddl_dag():

    @task
    def create_snowflake_table():
        return SnowflakeOperator(
            task_id="create_snowflake_table",
            snowflake_conn_id=SF_CONN,
            sql=create_ddl_table,
        )

    @task
    def get_mssql_table_definitions():
        # results = MSSQL_HOOK.get_records(get_table_definitions)
        df = MSSQL_HOOK.get_pandas_df(get_table_definitions)
        return df

    def load_table_definitions(df):
        table_name = df[0] 
        column_data = df[1] 
        # table_name = results['TableName']
        # column_data = results['ColumnData']
        sql = f"INSERT INTO {SNOWFLAKE_TABLE} VALUES ('{table_name}', '{column_data}')"
        return SnowflakeOperator(
            task_id="load_table_definitions",
            snowflake_conn_id=SF_CONN,
            sql= sql,
        )
    
    create_snowflake_table()
    get_mssql_table_definitions()

    # load = load_table_definitions(results=get_mssql_table_definitions())
    # create >> load


generate_ddl_dag()
