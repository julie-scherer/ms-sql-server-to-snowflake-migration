#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime
import pymssql 
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


def pymssql_func(**kwargs):
    conn = pymssql.connect(kwargs['server'], kwargs['user'], kwargs['pw'],kwargs['database']) 
    cursor = conn.cursor()
    print(f"kwargs_query: {kwargs['query']}")
    cursor.execute(kwargs['query'])
    row = cursor.fetchall()
    LD_QUERY=f"SELECT top 100 * FROM dbo.TransDetail WHERE CAST(DATE_ENTERED AS DATE) = '{kwargs['execution_date'].format('YYYY-MM-DD')}';"
    print(f'LD_QUERY:{LD_QUERY}')
    print(row)
    return 'welcome to SQLDWRPT'


with DAG(
    dag_id='example_python_operator',
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'python operator'],
    params={"example_key": "example_value"},
) as dag:
    
    pymssql_SQLRPT  = PythonOperator(
        task_id='pymssql_SQLRPT', 
        python_callable=pymssql_func,
        op_kwargs={'server':'10.1.1.11', 'user':'svc_airflow', 'pw':'Svz)VUGq3wpz9(d*', 'database':'CURODW','query':'SELECT TOP 10 * FROM dw.ALL_DATE_DIM;'},
        )

    # SQL TO S3
    CURODWSQL_to_s3_taskOperator = SqlToS3Operator(
        task_id='CURODWSQL_to_s3_taskOperator',
        sql_conn_id='mssql_sqldwrpt_conn',
        query="SELECT TOP 10000 * FROM dw.ALL_DATE_DIM;",
        file_format='csv',
        s3_bucket="s3-dev-etldata-001",
        s3_key="TransDetail_202211.csv",
        aws_conn_id="aws_s3_dev_conn",
        replace=True,
    )

    @task()
    def CURODW_to_s3():
        LD_QUERY="SELECT TOP 10000 * FROM dw.ALL_DATE_DIM;"
        S3_FILE_NAME="inbound/LD/2022/11/ALL_DATE_DIM_202211.csv"

        mssql_hook = MsSqlHook(mssql_conn_id='mssql_sqldwrpt_conn')
        s3_hook = S3Hook(aws_conn_id='aws_s3_dev_conn')
        df = mssql_hook.get_pandas_df(sql=LD_QUERY)
        print(df)
        df_byte = df.to_csv(header=True,).encode()
        s3_hook.load_bytes(bytes_data=df_byte, bucket_name="s3-dev-etldata-001", replace=True, key=S3_FILE_NAME)


    pymssql_SQLRPT
    CURODW_to_s3()
    CURODWSQL_to_s3_taskOperator