from datetime import datetime
from io import StringIO

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


CONN_ID = "awsmssql_creosql_creo_conn"

with DAG(
    dag_id="creo_mssql_conn",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['example']
) as dag:
    
    # AWS CREO SQL Connections
    testing_creo_connection = MsSqlOperator(
			mssql_conn_id=CONN_ID,
            sql="SELECT TOP 10 * FROM dbo.Campaign ",
            task_id="testing_creo_connection",
    )

    @task()
    def mssqlHook_func():
        # Schema is the database, not the actual schema.
        mssql = MsSqlHook(mssql_conn_id=CONN_ID)

        # This method (get_pandas_df) does not work with the regular mssql plugin
        df = mssql.get_pandas_df("SELECT TOP 10 * FROM dbo.Contact;")
        print(f"df_results : {df}")

        # All the default dbapihook methods works
        my_records = mssql.get_records("SELECT TOP 10 * FROM dbo.Message;")
        print(f"my_records_results : {my_records}")

        mssql_run = mssql.run("SELECT TOP 10 * FROM dbo.Campaign;")
        print(f"mssql_run_results : {mssql_run}")


    testing_creo_connection
    mssqlHook_func()