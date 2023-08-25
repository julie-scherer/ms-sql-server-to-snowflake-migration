from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

default_args = {
    "owner": "Business Insights",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # "pool": "default",
}
with DAG(
    "ssh",
    default_args=default_args,
    start_date=datetime(2022, 5, 1),
    catchup=False,
    schedule_interval=None,
) as dag:

    test_ssh = SSHOperator(
        task_id="ssh_test", command='echo "hello world"', ssh_conn_id="ssh_conn"
    )

    powershell_ssh = SSHOperator(
        task_id="ssh_powershell",
        command='"C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe" "C:\\Users\\Administrator\\hello.ps1"',
        ssh_conn_id="ssh_conn",
    )
    exe_ssh = SSHOperator(
        task_id="ssh_exe",
        command='"C:\\Users\\Administrator\\helloworld.exe"',
        ssh_conn_id="ssh_conn",
    )

    test_ssh >> powershell_ssh >> exe_ssh
