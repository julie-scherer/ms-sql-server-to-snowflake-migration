import os
from datetime import datetime
from airflow.decorators import dag
from airflow.utils import timezone

# Import Operator
from astro.constants import FileLocation, FileType
import astro.sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 5, 1),
    tags=["demo"],
)
def example_postgres_to_snowflake():
    t0 = aql.export_file(
        task_id="save_file",
        input_data=Table(
            conn_id="pg_conn",
            name="task_instance",
            metadata=Metadata(schema="public", database="postgres"),
        ),
        output_file=File(
            path="/tmp/myfile.parquet",
            filetype=FileType.PARQUET,
        ),
        if_exists="replace",
    )
    t1 = aql.load_file(
        input_file=t0,
        output_table=Table(name="task_instance", conn_id="snowflake_conn"),
    )

example_postgres_to_snowflake = example_postgres_to_snowflake()
