import os
import logging
from datetime import timedelta
import pendulum
import csv

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from include.functions import ms_teams_callback_functions


## Global variables 
author = 'Julie Scherer'

TABLE_NAME = "ARES.ETL.CM"
S3_BUCKET_NAME = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001
FILE_FORMAT = """
    TYPE = CSV
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 1
    NULL_IF = 'NULL'
"""

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="awsmssql_cansql_winchkcan_conn")
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

# - - - - - - - - - - - - - - - - - - - -

get_table_definitions = f"""

-- Create a temporary table to store the results
DROP TABLE IF EXISTS #TableStats;
CREATE TABLE #TableStats (
    [TableName] VARCHAR(128),
    [ColumnData] NVARCHAR(MAX)
);

-- Cursor to iterate through each table
DECLARE @TableName NVARCHAR(128);
DECLARE @ColumnData NVARCHAR(MAX);

DECLARE tableCursor CURSOR FOR
SELECT name FROM sys.tables;

OPEN tableCursor;
FETCH NEXT FROM tableCursor INTO @TableName;

-- Loop through each table
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ColumnData = (
        SELECT
            c.name AS column_name
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
            END) AS data_type
            ,(CASE WHEN c.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END) AS nullable
            ,(CASE
                WHEN c.collation_name = 'SQL_Latin1_General_CP1_CI_AS' THEN 'COLLATE "en-ci"'
                ELSE ''
            END) AS collation
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(@TableName)
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
    );

    -- Insert into #TableStats
    INSERT INTO #TableStats (TableName, ColumnData)
    VALUES (@TableName, @ColumnData);

    FETCH NEXT FROM tableCursor INTO @TableName;
END;

CLOSE tableCursor;
DEALLOCATE tableCursor;

-- Retrieve the results
SELECT * FROM #TableStats;

-- Clean up temporary table
DROP TABLE IF EXISTS #TableStats;
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
    "Generate_CM_DDL",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    catchup=False,
    schedule_interval="@once",
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def Generate_DDL_DAG():
    @task
    def create_temp_table():
        create_pretty_ddl_table = f"""
            CREATE OR REPLACE TABLE {TABLE_NAME}_DDL_TEMP (
                Table_Name VARCHAR(128),
                Column_Data VARCHAR
            );
        """
        get_sf_hook().run(create_pretty_ddl_table)

    @task
    def raw_mssql_to_s3():
        df = get_mssql_hook().get_pandas_df(get_table_definitions)
        logging.info(f"Pandas df: \n\n{df.head()}")

        # Convert ColumnData column to JSON string
        df["ColumnData"] = df["ColumnData"].apply(lambda x: x.replace('"', "'").replace('\\"',''))
        
        df_byte = df.to_csv(
            header=True,  # Data file needs to have a header row
            index=False,  # Exclude the row index from the CSV
            sep="|",  # Use the pipe symbol as the column separator
            na_rep='NULL',  # Replace missing values with 'NULL'
            doublequote=True,  # Enable double quoting for values
            quoting=csv.QUOTE_NONE,
        ).encode()

        get_s3_hook().load_bytes(
            bytes_data=df_byte,
            bucket_name=S3_BUCKET_NAME,
            key=f"inbound/CM/DDL/CM_DDL.csv",
            replace=True,
        )

    @task
    def load_s3_to_sf_temp_table():
        get_sf_hook().run(f"TRUNCATE TABLE IF EXISTS {TABLE_NAME}_DDL_TEMP")
        sql_query = f"""
        COPY INTO {TABLE_NAME}_DDL_TEMP
        FROM (
            SELECT 
                $1, $2
            FROM @ETL.INBOUND/CM/DDL/
        )
        FILE_FORMAT = ( 
            {FILE_FORMAT}
        ) 
        PATTERN = '.*CM_DDL.csv'    
        """
        get_sf_hook().run(sql_query)
    
    @task
    def create_pretty_ddl_table():
        query = f"""
        CREATE OR REPLACE TABLE {TABLE_NAME}_DDL (
            table_name VARCHAR,
            column_name VARCHAR,
            data_type VARCHAR,
            collation VARCHAR,
            nullable VARCHAR,
            definition VARCHAR
        );
        """
        get_sf_hook().run(query)
    
    @task
    def insert_into_ddl_table():
        query = f"""
        INSERT INTO {TABLE_NAME}_DDL (
            table_name,
            column_name,
            data_type,
            collation,
            nullable,
            definition
        )
        WITH ParsedData AS (
            SELECT 
                table_name,
                PARSE_JSON('[' || Column_Data || ']') AS json_array
            FROM {TABLE_NAME}_DDL_TEMP
        ),
        FlattenedData AS (
            SELECT
                table_name,
                flattened.value AS column_data
            FROM ParsedData,
            LATERAL FLATTEN(input => json_array) AS flattened
        ),
        SplitFields AS (
            SELECT
                table_name,
                column_data:column_name AS column_name,
                column_data:data_type AS data_type,
                column_data:collation AS collation,
                column_data:nullable AS nullable
            FROM FlattenedData
        )
        SELECT 
            table_name,
            column_name,
            data_type,
            collation,
            nullable,
            (column_name || ' ' || data_type || ' ' || collation || ' ' || nullable) AS definition
        FROM SplitFields;

        """
        get_sf_hook().run(query)
    
    @task
    def delete_temp_table():
        query = f"""
        DROP TABLE IF EXISTS {TABLE_NAME}_DDL_TEMP;
        """
        get_sf_hook().run(query)
    
    create_temp_table_task = create_temp_table()
    raw_mssql_to_s3_task = raw_mssql_to_s3()
    load_s3_to_sf_temp_table_task = load_s3_to_sf_temp_table()

    create_pretty_ddl_table_task = create_pretty_ddl_table()
    insert_into_ddl_table_task = insert_into_ddl_table()
    delete_temp_table_task = delete_temp_table()

    create_temp_table_task >> raw_mssql_to_s3_task >> load_s3_to_sf_temp_table_task >> \
    create_pretty_ddl_table_task >> insert_into_ddl_table_task >> delete_temp_table_task


generate_ddl_dag = Generate_DDL_DAG()


# git checkout AB#203810-Generate-DDL-DAG
# https://github.com/CuroFinTechCorp/Curo-Astro/blob/AB%23203810-Generate-DDL-DAG/dags/Generate_DDL_DAG.py