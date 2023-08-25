"""
### CREO DQ checks 
Triggered by.: STAGING_CREO DAG.
Description..: Data Quality checks for nightly loads.
Working......: Checks are made against Source database WinchCAN
               DAG dynamically creates checks_CREO.yml for all the tables in the dictionary. And calls SODA passing checks_CREO.yml. 
               Following checks are run against table dictionary (checks.yml):
                    #1 Missing(asofdate) = 0 Checks for nulls and blanks 
                    #2 Missing(filename) = 0 Checks for nulls and blanks
                    #3 duplicate_keys    = 0 Checks for a given key if tables has duplicate records
                    #4 record_count_differential < 10 Checks for record cound difference should not be greater than 10
Table dict...: for eg: 'CREO_InsuranceEnrollment_HIST':{"Key_column":'INSURANCE_ENROLLMENT_KEY', "date_field":True, "check_dupe":True}'
                    #1 and #2 Missing asofdate and filename are default 
                    #3 is based on table attribute
                    #4 count diff check is by default   
Upon Failure.: Sends notification alerts on Teams Channel.
"""

import logging
import pendulum
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG, macros, Dataset
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator 
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.bash import BashOperator

from include.functions import ms_teams_callback_functions

# - - - - - - - - - SETTINGS - - - - - - - - -

SODA_PATH="/usr/local/airflow/include/soda"
TABLE_LIST = ['APPROVALREQUEST', 'APPROVALREQUESTITEM', 'CAMPAIGN', 'CAMPAIGNTYPE', 'COMMUNICATION', 'COMMUNICATIONMAILING', 'CONFIG', 'CONFIGHISTORY', 'CONTACT', 'CONTACTTYPE', 'CONTAINER', 'DATASET', 'DATASETCELL', 'DATASETCOLUMN', 'DATASETROW', 'DATASETVALUE', 'DATASOURCE', 'DEADMESSAGES', 'DEADMESSAGES2', 'DELIVERYSTATUS', 'EMOJI', 'FOLDER', 'FOLDERCONTACT', 'FOLDERMESSAGE', 'GLOBAL', 'LOG', 'MESSAGE', 'MESSAGECONTACT', 'MESSAGECONTACTTYPE', 'MESSAGECONTACTV2', 'MESSAGEDELIVERYSTATUS', 'MESSAGEPART', 'MESSAGEPARTV2', 'MESSAGESTATUSQUEUE', 'MESSAGETYPE', 'PACKAGE', 'PACKAGETEMPLATE', 'PARAMETER', 'RULE', 'TEMPLATE', 'TEMPLATERULE', 'TEMPLATETYPE', 'TEMPMESSAGE', 'USER', 'WEBHOOK']

# Data driven scheduling datasets 
staging_dataset_file = "include/datasets/Staging_CREO_Dataset.txt"
staging_dataset_obj = Dataset(staging_dataset_file)

dq_dataset_file = "include/datasets/DQ_CREO_Dataset.txt"
dataset_path = Path(dq_dataset_file)
dataset_path.touch(exist_ok=True)
dq_dataset_obj = Dataset(dq_dataset_file)

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="awsmssql_creosql_creo_conn")
    return MSSQL_HOOK

def get_sf_hook():
    """
    Returns a Snowflake hook for interacting with Snowflake.
    """
    SF_HOOK = SnowflakeHook(snowflake_conn_id="snowflake_default")
    return SF_HOOK

# - - - - - - SNOWFLAKE UTILS TABLE - - - - - - -

def get_table_utils(table_name):
    utils_query = f"""
            SELECT * FROM ETL.CREO_UTILS
            WHERE table_name ILIKE 'CREO_{table_name}_HIST'
        """
    table_utils = get_sf_hook().get_first(utils_query)
    if not table_utils:
        raise ValueError(f"Table utilities not found. Check ETL table in Snowflake and function to retrieve utils in DAG.")
    
    return table_utils

# - - - - - - - - DATA QUALITY DAG - - - - - - - - -

## Default DAG args
default_args = {
    'owner': 'Data Engineering',
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=1),
    "on_failure_callback": ms_teams_callback_functions.failure_callback
}

with DAG(
    dag_id="DQ_CREO",
    catchup=False,
    start_date=pendulum.datetime(2023, 5, 5, tz='US/Eastern'),
    schedule=[staging_dataset_obj],
    doc_md=__doc__,
    tags=["Staging_CREO"],    
    default_args=default_args
) as dag:
    #Sets the values/parameters for the config file and the checks file. Single checks file will contain checks for all tables
    def set_config(ds = None, **kwargs):
        
        with open(f"{SODA_PATH}/config/configuration.yml", "r") as config_file:
            conf = config_file.read()
            conf_updated = conf\
                .format(
                    username = Variable.get("soda_username_var"), 
                    password = Variable.get("soda_pwd_var"), 
                    account = Variable.get("soda_account_var"), 
                    database = Variable.get("soda_database_var"),
                    warehouse = Variable.get("soda_warehouse_var"), 
                    role = Variable.get("soda_role_var"), 
                    schema = Variable.get("soda_schema_var")
            )
        with open(f"{SODA_PATH}/config/configuration_parametrized_CREO.yml","w") as config_file_updated:
            config_file_updated.write(conf_updated)
        logging.info("Updated the config file")

        with open(f"{SODA_PATH}/checks/checks.yml", "r") as file:
            dq = file.read() 

        today = datetime.strptime(ds, "%Y-%m-%d")
        logging.info(today)
        asOfDate = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        logging.info(asOfDate)
        
        with open(f"{SODA_PATH}/checks/checks_CREO.yml","a") as file2:
            for table_name in TABLE_LIST:
                table_params = get_table_utils(table_name)
                key = table_params.get("key_column")
                logging.info(f"The key for this table is {key}")

                df = None
                
                snowflake_query_min = f"""
                    SELECT MIN({key}) FROM STG.CREO_{table_name}_HIST 
                    WHERE asOfDate < '{asOfDate}'
                """
                min_value_df = get_sf_hook().get_pandas_df(snowflake_query_min)
                min_value = min_value_df.iloc[0,0]
                min_value = 0 if min_value == None else min_value

                snowflake_query_max = f"""
                    SELECT MAX({key}) FROM STG.CREO_{table_name}_HIST 
                """
                max_value_df = get_sf_hook().get_pandas_df(snowflake_query_max)
                max_value = max_value_df.iloc[0,0]
                max_value = 0 if max_value == None else max_value
                
                if table_params.get("table_filtered_by"):
                    date_column = table_params.get("table_filtered_by")
                    df = get_mssql_hook().get_pandas_df(sql=f"""
                        SELECT COUNT(*) 
                        FROM [dbo].[{table_name}]
                        WHERE CAST({date_column} AS DATE) = '{asOfDate}' 
                            AND {key} > {min_value} 
                            AND {key} <= {max_value};
                    """)
                else:
                    df = get_mssql_hook().get_pandas_df(sql=f"""
                        SELECT count(*) 
                        FROM dbo.{table_name}
                        WHERE {key} > {min_value} AND {key} <= {max_value};
                    """)
                
                count = df.iloc[0,0]
                count = 0 if count == None else count
                
                custom_dq_checks = None
                custom_dq_checks = dq.format(
                    table=f"CREO_{table_name}_HIST", 
                    md=asOfDate, 
                    expected=count
                )
                
                logging.info(custom_dq_checks)
                file2.write(custom_dq_checks)

    #Safety check to make sure that both the configuration file and the checks files exists and has been created successfully
    def check_config_exists(**kwargs):
        checks_file_name = f"{SODA_PATH}/checks/checks_CREO.yml"
        with open(checks_file_name,"r") as checks_file:
            logging.info(f"Printing checks contents: \n{checks_file.read()}")

    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "end"]]
    
    #Operators for creating and cleaning up the initial checks file. Just an empty file is sufficient as it will have its values/parameters dynamically written in during the set config phase
    remove_checks = BashOperator(
        task_id='remove_checks',
        bash_command='rm -f /usr/local/airflow/include/soda/checks/checks_CREO.yml',
    )

    create_checks = BashOperator(
        task_id='create_checks',
        bash_command='touch /usr/local/airflow/include/soda/checks/checks_CREO.yml',
    )
    
    safety_check = PythonOperator(
        task_id='check_config_exists', 
        python_callable=check_config_exists,
        op_kwargs=None,
        retries = 3,
        retry_delay=timedelta(minutes=5),
    )

    set_conf = PythonOperator(
        task_id='set_conf', 
        python_callable=set_config,
        op_kwargs=None,
    )
    
    #Operator for running the soda scan
    soda_scan = BashOperator(
        task_id="dq_CREO",
        bash_command=f"soda scan -d ds -c {SODA_PATH}/config/configuration_parametrized_CREO.yml {SODA_PATH}/checks/checks_CREO.yml"
    )

    @task(outlets=[dq_dataset_obj])
    def write_to_dataset():
        with open(dq_dataset_file, "a") as f:
            f.write("CREO DQ complete")
            logging.info("Updated the dataset file")

start >> remove_checks >> create_checks >> set_conf >> safety_check >> soda_scan >> write_to_dataset() >> end
