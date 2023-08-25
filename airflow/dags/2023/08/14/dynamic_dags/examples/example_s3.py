
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListPrefixesOperator
from airflow.models import Variable

dev_s3_bucket= Variable.get("s3_etldata_bucket_var")

with DAG(dag_id='example_s3',
         start_date=datetime(2022, 7, 1),
         schedule_interval=None,
         ) as dag:
        
    list_prefixes = S3ListPrefixesOperator(
        task_id="list_prefix",
        bucket=dev_s3_bucket,
        prefix='inbound/',
        delimiter='/',
        aws_conn_id='aws_s3_conn',

    )
    list_s3_files = S3ListOperator(
        task_id='list_s3_files',
        bucket=dev_s3_bucket,     # The S3 bucket where to find the objects
        prefix='inbound/test/',          # Prefix string to filters the objects whose name begin with such prefix.
        delimiter='/',                   # The delimiter marks key hierarchy.
        aws_conn_id='aws_s3_conn'
    )

    @task(multiple_outputs=True)
    def printName():
        s3_bucket = s3_bucket= Variable.get("s3_etldata_bucket_var")
        print(s3_bucket)
        print(f"DEV_S3_BUCKET: {s3_bucket}")
        return {"s3_bucket":s3_bucket}
    printName()
