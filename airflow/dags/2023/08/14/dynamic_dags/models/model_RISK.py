"""
### Model DW DAG 
Triggered by.: SUCCESSFUL completion of the DQ CS CM and DQ CS LD dags. <br>
Description..: Runs the stored procedures for loading data to the TU Credit report tables in RISK schema. <br>
Working......: Returns True upon successful run and will also log the number of new records inserted <br>
               as well as the number of records updated. <br>
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

dq_cm_cs_ds_name = "include/datasets/ds_dq_cs_cm.txt"
ds_DQ_CS_CM_DAG = Dataset(dq_cm_cs_ds_name)

dq_ld_cs_ds_name = "include/datasets/ds_dq_cs_ld.txt"
ds_DQ_CS_LD_DAG = Dataset(dq_ld_cs_ds_name)

ds_risk_name = "include/datasets/ds_model_risk.txt"
ds_risk_DAG = Dataset(ds_risk_name)

snowflake = "snowflake_default"

procedures = {
    'SP_TU_CreditReport_AddressFeatures',
    'SP_TU_CreditReport_CollectionFeatures',
    'SP_TU_CreditReport_Scores'
}  

procedured_fico_V8 = {
    'SP_Score_FICO_V8'
}

@dag(
    "model_risk",
    start_date=pendulum.datetime(2023, 6, 30, tz='US/Eastern'),
    catchup=False,
    schedule = [ds_DQ_CS_CM_DAG, ds_DQ_CS_LD_DAG],
    #schedule_interval = None,
    dagrun_timeout = timedelta(hours=2),
    doc_md=__doc__,
    tags=["Author: Shunjian Wang","cross-DAG dependencies", "DQ_CM_CS", "DQ_LD_CS"],
    default_args={
        "retries": 0,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        'execution_timeout': timedelta(hours=1),
        "on_failure_callback": ms_teams_callback_functions.failure_callback
    })

def Model_RISK():
    start, mid1, mid2, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "mid1", "mid2", "end"]]

    @task()
    def run_procedure(proc_name, ds=None, ti=None, **kwargs) -> bool:
        #Running procedures sequentially for now
        sql_formatted = None
        logging.info("Running stored procedure {db}.{procedure}".format(db="RISK",procedure=proc_name))
        sql='call {database}.{procedure}();'
        sql_formatted = sql.format(database="RISK", procedure = proc_name)
            
        logging.info(sql_formatted)
        sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
        logging.info("Snowflake hook created")
        logging.info("Prepareing to execute the store procedure")
        df = sf_hook.get_pandas_df(sql_formatted)
        result = df.iloc[0,0]
        logging.info(result)
        
        return True
    
    @task(outlets=[ds_risk_DAG])
    def write_to_dataset():
        f = open(ds_risk_name, "a")
        f.write("data successfully loaded to TU risk tables")
        f.close()

    for proc in procedures:
        #with TaskGroup(group_id="tg") as tg:  #tg_CM
        tg_id="tg_{}".format(proc)
        with TaskGroup(group_id=tg_id) as tg:
            run_procedure(proc)
        start >> tg >> mid1
    
    for proc in procedured_fico_V8:
        #with TaskGroup(group_id="tg") as tg:  #tg_CM
        tg_id="tg_{}".format(proc)
        with TaskGroup(group_id=tg_id) as tg:
            run_procedure(proc)
        mid1 >> tg >> mid2
    
    mid2 >> write_to_dataset() >> end
model_dw_run = Model_RISK()