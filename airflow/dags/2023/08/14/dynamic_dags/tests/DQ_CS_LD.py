"""
### LD DQ checks 
Triggered by.: STAGING_LD_CS DAG.
Description..: Data Quality checks for nightly loads.
Working......: Checks are made against Source database Customer Scoring
               DAG dynamically creates checks_LD_CS.yml for LD_AgencyData_HIST. And calls SODA passing checks_LD_CS.yml. 
               Following checks are run against table dictionary (checks.yml):
                    #1 Missing(asofdate) = 0 Checks for nulls and blanks 
                    #2 Missing(filename) = 0 Checks for nulls and blanks
                    #4 record_count_differential < 10 Checks for record cound difference for a given date range should not be greater than 10  
Upon Failure.: Sends notification alerts on Teams Channel.
"""
from airflow import DAG, macros, Dataset
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils import trigger_rule

from include.functions import ms_teams_callback_functions
from airflow.operators.bash import BashOperator
import pendulum
import numpy as np
import logging

db_name = "awsmssql_canldsql_customerscoring_conn"
snowflake = "snowflake_default"

staging_ld_cs_dataset = "include/datasets/ds_staging_LD_CS.txt"
ds_Staging_LD_CS = Dataset(staging_ld_cs_dataset)

dq_ld_cs_ds_name = "include/datasets/ds_dq_cs_ld.txt"
ds_DQ_CS_LD_DAG = Dataset(dq_ld_cs_ds_name)

fullTableList = {'LD_CS_AgencyData_HIST'}

with DAG(
    dag_id="DQ_LD_CS",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 18, tz='US/Eastern'),
    schedule = [ds_Staging_LD_CS],
    #schedule_interval = None, # DAG Time Zone set to EST 
    doc_md=__doc__,
    tags=["Author: Shunjian Wang","cross-DAG dependencies", "Staging_LD_CS"],    
    default_args={
        "retries": 1,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=1)
        ,"on_failure_callback": ms_teams_callback_functions.failure_callback
    }
) as dag:
    SODA_PATH="/usr/local/airflow/include/soda"

    def set_config(ds = None, **kwargs):
        config_file = open("{directory}/config/configuration.yml".format(directory = SODA_PATH),"r")
        conf = config_file.read()
        conf_updated = conf.format(username = Variable.get("soda_username_var"), password = Variable.get("soda_pwd_var"), account = Variable.get("soda_account_var"), database = Variable.get("soda_database_var"), warehouse = Variable.get("soda_warehouse_var"), role = Variable.get("soda_role_var"), schema = Variable.get("soda_schema_var"))
        config_file_updated = open("{directory}/config/configuration_parametrized_LD.yml".format(directory = SODA_PATH),"w")
        config_file_updated.write(conf_updated)
        config_file.close()
        config_file_updated.close()
        logging.info("Updated the config file")

        file = open("{directory}/checks/{filename}".format(directory = SODA_PATH, filename = "checks_CS.yml"), "r")
        dq = file.read() 
        file.close()
        today = datetime.strptime(ds, "%Y-%m-%d")
        asOfDate = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        logging.info(today)
        logging.info(asOfDate)
        file2 = open(f"{SODA_PATH}/checks/checks_LD_CS.yml","a")

        for table in fullTableList:

            table_name = table
            mssql_hook = MsSqlHook(mssql_conn_id=db_name)
            df = None
            
            df = mssql_hook.get_pandas_df(sql="select count(*) FROM dbo.{t} WHERE CAST(DATE_ENTERED AS DATE) = '{target_date}';".format(t=table_name.replace("LD_CS_","")[:-5], target_date = asOfDate))
            
            count = df.iloc[0,0]
            if count == None:
                count = 0
            
            custom_dq_checks = None
            custom_dq_checks = dq.format(md = asOfDate, expected = count, table = table)
            
            logging.info(custom_dq_checks)
            
            file2.write(custom_dq_checks)             
        
        file2.close()

    def check_config_exists(**kwargs):
        checks_file_name = f"{SODA_PATH}/checks/checks_LD_CS.yml"
        checks_file = open(checks_file_name,"r")
        logging.info("Printing checks contents")
        logging.info(checks_file.read())
        checks_file.close()

    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "end"]]
    
    #Operators for creating and cleaning up the initial checks file. Just an empty file is sufficient as it will have its values/parameters dynamically written in during the set config phase
    remove_checks = BashOperator(
        task_id='remove_checks',
        bash_command='rm -f /usr/local/airflow/include/soda/checks/checks_LD_CS.yml',
    )

    create_checks = BashOperator(
        task_id='create_checks',
        bash_command='touch /usr/local/airflow/include/soda/checks/checks_LD_CS.yml',
    )
    
    safety_check = PythonOperator(
        task_id='check_config_exists', 
        python_callable=check_config_exists,
        op_kwargs=None,
        retries = 3,
        retry_delay = timedelta(minutes=5),
    )

    set_conf = PythonOperator(
        task_id='set_conf', 
        python_callable=set_config,
        op_kwargs=None,
    )
    
    #Operator for running the soda scan
    soda_scan = BashOperator(
        task_id="dq_LD_CS",
        bash_command=f"soda scan -d ds -c {SODA_PATH}/config/configuration_parametrized_LD.yml {SODA_PATH}/checks/checks_LD_CS.yml"
    )

    @task(outlets=[ds_DQ_CS_LD_DAG])
    def write_to_dataset():
        f = open(dq_ld_cs_ds_name, "a")
        f.write("ld dq check complete for customer scoring db")
        f.close()

start >> remove_checks >> create_checks >> set_conf >> safety_check >> soda_scan >> write_to_dataset() >> end