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
SNOWFLAKE_TABLE_NAME = "OUT_CM_CU"
S3_CONN_ID = "aws_s3_dev_conn"
S3_BUCKET_NAME = "s3-dev-etldata-001"
S3_FILE_NAME = "inbound/test/Out_CM_CU.csv"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "ARES"
SNOWFLAKE_SCHEMA = "STG"

default_args = {
        "owner": "cs",
        "retries": 1,
        "retry_delay": timedelta(seconds=15),
    }

with DAG(dag_id="example_s3_to_snowflake",
         start_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
         schedule_interval=None,
         default_args=default_args,
         tags=["AstroSDK", "Astronomer", "S3", "Snowflake"],
         ) as dag:

    # Make sure to have database and schema added in your connection
    s3_to_snowflake = aql.load_file(
        task_id="s3_to_snowflake",
        input_file=File(path=f"s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}", filetype=FileType.CSV),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA),
            name=SNOWFLAKE_TABLE_NAME,
        ),
        if_exists="replace",
        use_native_support=False,
        enable_native_fallback=False
    )

    aql.cleanup()

    s3_to_snowflake 

