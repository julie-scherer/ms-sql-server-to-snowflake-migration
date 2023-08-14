"""
### Microsoft Teams Callback
Shows how success and failures callbacks can be used to send notifications to MS Teams.
Also shows the callbacks being used with the `sla_miss` parameter.
"""

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from include.functions import ms_teams_callback_functions

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    # Callbacks set in default_args will apply to all tasks unless overridden at the task-level
    "on_success_callback": ms_teams_callback_functions.success_callback,
    "on_failure_callback": ms_teams_callback_functions.failure_callback,
    "on_retry_callback": ms_teams_callback_functions.retry_callback,
    "sla": timedelta(seconds=10),
}

with DAG(
    dag_id="example_ms_teams_callback_conn",
    default_args=default_args,
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(days=1),
    # sla_miss only applies to scheduled DAG runs, it does not work for manually triggered runs
    # If a DAG is running for the first time and sla is missed, sla_miss will not fire on that first run
    sla_miss_callback=ms_teams_callback_functions.sla_miss_callback,
    doc_md=__doc__,
    catchup=False,
) as dag:
    # This task uses on_execute_callback set in the default_args to send a notification when the task begins
    # and overrides the on_success_callback to None
    dummy_dag_triggered = DummyOperator(
        task_id="dummy_dag_triggered",
        on_execute_callback=ms_teams_callback_functions.dag_triggered_callback,
        on_success_callback=None,
    )

    # This task uses the default_args on_success_callback
    dummy_task_success = DummyOperator(
        task_id="dummy_task_success",
    )

    # This task sends a Teams message via a python_callable
    ms_teams_python_op = PythonOperator(
        task_id="ms_teams_python_op",
        python_callable=ms_teams_callback_functions.python_operator_callback,
        on_success_callback=None,
    )

    # Task will sleep beyond the 10 second SLA to showcase sla_miss callback
    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",
    )

    # Task will retry once before failing to showcase on_retry_callback and on_failure_callback
    bash_fail = BashOperator(
        task_id="bash_fail",
        bash_command="exit 123",
    )

    # Task will still succeed despite previous task failing by using trigger_rule, showcasing use of the
    # last task in a DAG to notify that the DAG has completed
    dummy_dag_success = DummyOperator(
        task_id="dummy_dag_success",
        on_success_callback=ms_teams_callback_functions.dag_success_callback,
        trigger_rule="all_done",
    )
    
    # Bad SQLDWRPT Connections    
    example_mssql_AWS_SQLRPTDW_conn = MsSqlOperator(
			mssql_conn_id="mssql_sqldwrpt_conn",
            sql="SELECT TOP 10 * FROM dw.ALL_DATE_DIM;",
            task_id="example_mssql_AWS_SQLRPTDW_conn",
    )
    
    # CM Connections
    example_mssql_AWS_CANCM_WinChkCAN_conn = MsSqlOperator(
			mssql_conn_id="mssql_cansqlrpt_winchkcan_conn",
            sql="SELECT top 10 * from dbo.ACH_History;",
            task_id="example_mssql_AWS_CANCM_WinChkCAN_conn"
    )
    
    # Good SQLDWRPT Connections
    example_mssql_AWS_SQLRPTDW_conn_good = MsSqlOperator(
			mssql_conn_id="mssql_sqldwrpt_curodw_conn",
            sql="SELECT TOP 10 * FROM dw.ALL_DATE_DIM;",
            task_id="example_mssql_AWS_SQLRPTDW_conn_good"
    )

    dummy_dag_triggered
    dummy_task_success
    ms_teams_python_op
    bash_sleep
    bash_fail
    dummy_dag_success
    example_mssql_AWS_SQLRPTDW_conn 
    example_mssql_AWS_CANCM_WinChkCAN_conn 
    example_mssql_AWS_SQLRPTDW_conn_good