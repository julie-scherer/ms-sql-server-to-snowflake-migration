from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    dag_id="example_snowflake",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None
) as dag:

    test_connection = SnowflakeOperator(
        snowflake_conn_id='snowflake_default',
        task_id='test_connection',
        sql='SELECT * FROM ETL.GETALLSCHEMAOBJECTS_VW;'
    )