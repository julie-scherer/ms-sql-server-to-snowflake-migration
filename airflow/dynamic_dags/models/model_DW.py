"""
### Model DW DAG 
Triggered by.: SUCCESSFUL completion of the DQ CM and DQ LD dags. <br>
Description..: Runs the stored procedures for loading data to each of the dimension and fact tables. <br>
Working......: Returns True upon successful run and will also log the number of new records inserted <br>
               as well as the number of records updated. <br>
               Please note that the record count may NOT always match up exactly with what is in WinchkCAN <br>
               and WinchkCANLD as the processes on wherescape are set to run every 3 days whereas this process <br>
               will trigger every day because dimension tables can be run independently while fact tables are <br> 
               dependent on the dimension tables, this dag is a 2 step process. Dimension tables will ALWAYS be run BEFORE fact tables. <br>
               If the process failed in the dimension table process, the fact table process will NOT run as to avoid creating incorrect data in the target db. <br>
               <br>
Upon Failure.: Sends notification alerts on Teams Channel. <br>
"""
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
import dateutil.relativedelta
import os
import numpy as np
from airflow import DAG, macros, Dataset
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils import trigger_rule
import logging
from include.functions import ms_teams_callback_functions
import pendulum


# Data driven scheduling datasets 
dq_cm_ds_name = "include/datasets/ds_dq_cm.txt"
dq_ld_ds_name = "include/datasets/ds_dq_ld.txt"
ds_DQ_CM_DAG = Dataset(dq_cm_ds_name)
ds_DQ_LD_DAG = Dataset(dq_ld_ds_name)

snowflake = "snowflake_default"

procedures = {
    'SP_CA_Customer_Dim':{"has_date":False},
    'SP_CA_Loans_Dim':{"has_date":False}, 
    'SP_CA_Stores_Dim':{"has_date":True}
}              
procedures_fact = {
    'SP_CA_LOANORIGINATIONS_FACT':{"has_date":False},
    'SP_CA_LOANBALANCES_FACT':{"has_date":True}
}

#There is no schedule interval so this dag will NEVER run on its own at a given time every day
#HOWEVER there is a schedule parameter which is set based on when a certain file(s) in local is updated
#DO NOT try triggering this DAG manually OR attempt to modify any of the files that this dag depends on
@dag(
    "model_dw",
    start_date=pendulum.datetime(2023, 6, 30, tz='US/Eastern'),
    catchup=False,
    schedule = [ds_DQ_CM_DAG, ds_DQ_LD_DAG],
    #schedule_interval = None,
    dagrun_timeout = timedelta(hours=2),
    doc_md=__doc__,
    tags=["Author: Shunjian Wang","cross-DAG dependencies", "DQ_CM", "DQ_LD"],
    default_args={
        "retries": 0,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        'execution_timeout': timedelta(hours=1),
        "on_failure_callback": ms_teams_callback_functions.failure_callback
    })

def Model_DW():
    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "end"]]

    # Triggering the stored procedures for dimension tables. As not ALL dimension tables 
    # require a start and end date, this method will only use the date parameters for the 
    # procedures which require it
    def run_procedure(ds=None, ti=None, **kwargs) -> bool:

        for proc in procedures:
            sql_formatted = None
            if(procedures[proc]["has_date"]==True):
                logging.info("Procedure {} requires a start and end date".format(proc))
                sql='call {database}.{procedure}(\'{start_date}\',\'{end_date}\');'
                aod = datetime.strptime(ds, "%Y-%m-%d")+timedelta(days=-1)
                logging.info(aod)
                next = datetime.strptime(ds, "%Y-%m-%d")
                logging.info(next)
                sql_formatted = sql.format(database="DW", procedure = proc, start_date=aod, end_date=next)
            else:
                logging.info("Procedure {} does NOT require a start and end date".format(proc))
                sql='call {database}.{procedure}();'
                sql_formatted = sql.format(database="DW", procedure = proc)
            
            logging.info(sql_formatted)
            sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
            logging.info("Snowflake hook created")
            logging.info("Prepareing to execute the store procedure")
            df = sf_hook.get_pandas_df(sql_formatted)
            result = df.iloc[0,0]
            logging.info(result)

        return True

    #Running the stored procedures for the fact tables. SO far ALL fact tables require a start and end date
    def run_procedure_facts(ds=None, ti=None, **kwargs) -> bool:

        for proc in procedures_fact:
            sql_formatted = None
            if(procedures_fact[proc]["has_date"]==False):
                logging.info("Procedure {} does NOT require a start and end date".format(proc))
                sql='call {database}.{procedure}();'
                sql_formatted = sql.format(database="DW", procedure = proc)
            else:
                logging.info("Procedure {} requires a start and end date".format(proc))
                sql='call {database}.{procedure}(\'{start_date}\',\'{end_date}\');'
                aod = datetime.strptime(ds, "%Y-%m-%d")+timedelta(days=-1)
                logging.info(aod)
                next = datetime.strptime(ds, "%Y-%m-%d")
                logging.info(next)
                sql_formatted = sql.format(database="DW", procedure = proc, start_date=aod, end_date=next)
            
            logging.info(sql_formatted)
            sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
            logging.info("Snowflake hook created")
            logging.info("Prepareing to execute the store procedure")
            df = sf_hook.get_pandas_df(sql_formatted)
            result = df.iloc[0,0]
            logging.info(result)

        return True
    
    run_proc_dim  = PythonOperator(
        task_id='dim_run', 
        python_callable=run_procedure,
        op_kwargs=None,
    )
    
    run_proc_fact  = PythonOperator(
        task_id='fact_run', 
        python_callable=run_procedure_facts,
        op_kwargs=None,
    )

    start >> run_proc_dim >> run_proc_fact >> end
model_dw_run = Model_DW()