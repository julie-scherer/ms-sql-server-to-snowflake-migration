import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

from astro import sql as aql
from astro.files import File, get_file_list
from astro.sql.table import Metadata, Table
from astro.constants import FileType


# To be changed accordingly
MSSQL_CONN_ID = "my_mssql_conn"
MSSQL_TABLE_NAME = SNOWFLAKE_TABLE_NAME = "countries"
S3_CONN_ID = "aws_default"
S3_BUCKET_NAME = "astro-onboarding"
S3_FILE_NAME = "countries.json"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "cs"
SNOWFLAKE_SCHEMA = "demo"

default_args = {
        "owner": "cs",
        "retries": 1,
        "retry_delay": timedelta(seconds=15),
    }

with DAG(dag_id="mssql_to_s3_to_snowflake",
         start_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
         schedule_interval=None,
         default_args=default_args,
         tags=["AstroSDK", "Astronomer", "MSSQL", "S3", "Snowflake"],
         ) as dag:

    create_table_mssql = MsSqlOperator(
        task_id="create_table_mssql",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=f"""
        CREATE TABLE {MSSQL_TABLE_NAME} (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            country_code TEXT,
            name TEXT,
            language TEXT
        );
        """,
    )

    @task()
    def insert_data_mssql():
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        rows = [
            ("PL", "Poland", "Polish"),
            ("FR", "France", "French"),
            ("JP", "Japan", "Japanese"),
        ]
        target_fields = ["country_code", "name", "language"]
        mssql_hook.insert_rows(table=MSSQL_TABLE_NAME, rows=rows, target_fields=target_fields)

    @task
    def mssql_to_s3():
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        df = mssql_hook.get_pandas_df(sql=f"SELECT * FROM {MSSQL_TABLE_NAME}")
        df_byte = df.to_json().encode()
        s3_hook.load_bytes(bytes_data=df_byte, bucket_name=S3_BUCKET_NAME, replace=True, key=S3_FILE_NAME)

    # Make sure to have database and schema added in your connection
    s3_to_snowflake = aql.load_file(
        task_id="s3_to_snowflake",
        input_file=File(path=f"s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}", filetype=FileType.JSON),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA),
            name=SNOWFLAKE_TABLE_NAME,
        ),
        if_exists="replace",
    )

    create_table_mssql >> insert_data_mssql() >> mssql_to_s3() >> s3_to_snowflake

