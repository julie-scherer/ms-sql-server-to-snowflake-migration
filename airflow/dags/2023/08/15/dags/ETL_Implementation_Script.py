"""
### Implementation Script to be ran against Snowflake
This DAG picks up .sql files with a format of 'YYYYMMDD_ScriptName' to be executed in the Snowflake environment. This dag is set to run at 1 AM EST daily.
Eg. If the dag is set to run on '20230306', the scripts it would look to pick up are scripts dated '20230306' and any scripts with that date would be executed in Snowflake.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta
from datetime import date, datetime
import os

from functools import partial
from airflow.utils import trigger_rule
from include.functions import ms_teams_callback_functions
import pendulum

sql_script_path = "/usr/local/airflow/include/sql/ImplementationScripts/"

with DAG('ETL_Implementation_Script',
    start_date=pendulum.datetime(2023, 3, 1, tz='US/Eastern'),
    catchup=False,
    schedule_interval = '0 1 * * *', # DAG Time Zone set to EST  
    dagrun_timeout = timedelta(hours=2),
    doc_md=__doc__,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": ms_teams_callback_functions.failure_callback
    }) as dag:

    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_done") for tid in ["start", "end"]]

    end.trigger_rule = trigger_rule.TriggerRule.ALL_DONE

    def get_script_task(sql_script_path, ds=None): 
        #Getting the actual date of Dag run
        GetDate = datetime.strptime(ds, "%Y-%m-%d")
        NowGetDate = GetDate.strftime('%Y%m%d')
        sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        print(f"Dag is looking for scripts with the current rundate of: {NowGetDate}")

        #Getting the sql file path
        if not os.path.isdir(sql_script_path):
            print("This job ran fine, no file to pick up today.")
            pass
        else:
            sqlfiles = os.listdir(sql_script_path)
            print(f"List files in dir: {sqlfiles}")

            #Looping through the filepath for files with the current dag run date and executing each query within each file.
            for file in sqlfiles:
                if NowGetDate in file:
                    print(f"Dag is now opening this file because it matches the current rundate: {file}")
                    queryfile = open(sql_script_path+file, "r")
                    mssql_query = queryfile.read()
                    sqlCommands = mssql_query.split(';')
                    queryfile.close()

                    for sqlQuery in sqlCommands:
                        print("Dag is now executing query/queries in current file")
                        try:
                            sf_hook.run(sqlQuery)
                        except: 
                            print(f"This Sql Query Failed: {sqlQuery}")
                else: 
                    print("No files matched the current rundate; No files were picked up. Dag was executed succesfully!")

    #Execute get script task
    ExecSqlScript = PythonOperator(
        task_id = f'ExecSqlScript',
        python_callable = get_script_task,
        retries = 0,
        op_kwargs = {'sql_script_path': sql_script_path}
    )
    
start >> ExecSqlScript >> end
    
    
 

   #  @task()
  #   def get_script_task():


