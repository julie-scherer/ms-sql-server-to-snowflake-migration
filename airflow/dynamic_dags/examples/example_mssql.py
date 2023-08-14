from datetime import datetime
from io import StringIO

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

with DAG(
    dag_id="example_mssql",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['example']
) as dag:
    
## connections naming convention - (for example - awsmssql_canldsql_winchkcanld_conn )
## mssql or awsmssql - on prem or on AWS 
## servername - canldsql    (AWS server names are long url)
## database   - winchkcanld (There might be more than one database on server, will have 1 conn per DB
## suffix     - conn        (Conn to show its a connection, var to show its a variable eg. S3 bucket name)    
    
    # CURODW 
    example_mssql_AWS_SQLDW_conn = MsSqlOperator(
			mssql_conn_id="mssql_sqldw_curodw_conn",
            sql="SELECT TOP 10 * FROM dw.ALL_DATE_DIM;",
            task_id="example_mssql_AWS_SQLDW_conn",
    )
    
    # AWS Cash Money - WinchkCAN database SQL Connections
    example_AWSmssql_CANSQL_WinChkCAN_conn = MsSqlOperator(
			mssql_conn_id="awsmssql_cansql_winchkcan_conn",
            sql="SELECT TOP 10 * FROM dbo.TransDetail ",
            task_id="AWSmssql_CANSQL_WinChkCAN_conn",
    )
    # AWS LD SQL Connections
    example_AWSmssql_CANLD_WinChkCANLD_conn = MsSqlOperator(
			mssql_conn_id="awsmssql_canldsql_winchkcanld_conn",
            sql="SELECT TOP 10 * FROM dbo.TransDetail ",
            task_id="AWSmssql_CANLD_WinChkCANLD_conn",
    )
    
    # AWS CREO SQL Connections
    example_AWSmssql_CREO_conn = MsSqlOperator(
			mssql_conn_id="awsmssql_creosql_creo_conn",
            sql="SELECT TOP 10 * FROM dbo.Campaign ",
            task_id="AWSmssql_CREO_conn",
    )

    # AWS CM RiskAnalytics SQL Connections
    example_AWSmssql_CM_RA_conn = MsSqlOperator(
			mssql_conn_id="awsmssql_cansql_riskanalytics_conn",
            sql="SELECT TOP 10 * FROM VEN.Score_FICO_V8 ",
            task_id="AWSmssql_CM_RA_conn",
    )

    # AWS LD RiskAnalytics SQL Connections
    example_AWSmssql_LD_RA_conn = MsSqlOperator(
			mssql_conn_id="awsmssql_canldsql_riskanalytics_conn",
            sql="SELECT TOP 10 * FROM VEN.Score_FICO_V8 ",
            task_id="AWSmssql_LD_RA_conn",
    )
    
    # Ivanti CSM SQL Connections
    example_mssql_ivanti_csm_conn = MsSqlOperator(
			mssql_conn_id="mssql_ivanti_csm_conn",
            sql="SELECT TOP 10 * FROM dbo.Change ",
            task_id="mssql_ivanti_csm_conn",
    )
    
    # Dynamics CMCCApp SQL Connections
    example_mssql_ictdynsqlrpt_cmccapp_conn = MsSqlOperator(
			mssql_conn_id="mssql_ictdynsqlrpt_cmccapp_conn",
            sql="SELECT TOP 5 * FROM dbo.Account ",
            task_id="mssql_ictdynsqlrpt_cmccapp_conn",
    )

    # Dynamics SCHCApp SQL Connections
    example_mssql_ictdynsqlrpt_schcapp_conn = MsSqlOperator(
			mssql_conn_id="mssql_ictdynsqlrpt_schcapp_conn",
            sql="SELECT TOP 5 * FROM dbo.Account ",
            task_id="mssql_ictdynsqlrpt_schcapp_conn",
    )
    # Verge Credit SQL Connections
    example_mssql_ictvcsql_winchkictvc_conn = MsSqlOperator(
            mssql_conn_id="mssql_ictvcsql_winchkictvc_conn",
            sql="SELECT TOP 5 * FROM dbo.ACHBank ",
            task_id="example_mssql_ictvcsql_winchkictvc_conn",
    )

    example_mssql_AWS_SQLDW_conn
    example_AWSmssql_CANSQL_WinChkCAN_conn   
    example_AWSmssql_CANLD_WinChkCANLD_conn
    example_AWSmssql_CREO_conn
    example_AWSmssql_CM_RA_conn
    example_AWSmssql_LD_RA_conn
    example_mssql_ivanti_csm_conn
    example_mssql_ictdynsqlrpt_cmccapp_conn
    example_mssql_ictdynsqlrpt_schcapp_conn
    example_mssql_ictvcsql_winchkictvc_conn
