"""
### CREO DQ checks 
Triggered by.: STAGING_CREO DAG.
Description..: Data Quality checks for nightly loads.
Working......: Checks are made against Source database CREO
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
import pandas as pd

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

tableList = {
    ## >> #1 Custom SQL Loads 
    "CREO_MESSAGE_HIST": {"sql": "COPY_CREO_MESSAGE.sql", "key_column": "MESSAGE_KEY", "keys": ["CONTAINER_KEY", "TEMPLATE_KEY", "SEND_AFTER_MESSAGE_KEY", "DELIVERY_STATUS_KEY", "IN_REPLY_TO_MESSAGE_KEY", "COMMUNICATION_MAILING_KEY", "MESSAGE_KEY", "DATASET_ROW_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    ## << #1 Custom SQL Loads 

    ## >> #2 Incremental load using Date_Entered
    "CREO_APPROVALREQUEST_HIST": {"sql": None, "key_column": "APPROVAL_REQUEST_KEY", "keys": ["PACKAGE_KEY", "APPROVAL_REQUEST_KEY"], "table_filtered_by": "ENTERED_AT"}, 
    "CREO_CAMPAIGN_HIST": {"sql": None, "key_column": "CAMPAIGN_KEY", "keys": ["CONTAINER_KEY", "CAMPAIGN_TYPE_KEY", "CAMPAIGN_KEY", "SEED_LIST_DATASET_KEY", "DATASET_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_COMMUNICATION_HIST": {"sql": None, "key_column": "COMMUNICATION_KEY", "keys": ["CAMPAIGN_KEY", "COMMUNICATION_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_COMMUNICATIONMAILING_HIST": {"sql": None, "key_column": "COMMUNICATION_MAILING_KEY", "keys": ["COMMUNICATION_MAILING_KEY", "COMMUNICATION_KEY"], "table_filtered_by": "DATE_COMPLETED"}, 
    "CREO_CONFIGHISTORY_HIST": {"sql": None, "key_column": "CONFIG_HISTORY_KEY", "keys": ["CONFIG_HISTORY_KEY", "CONFIG_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_CONTACT_HIST": {"sql": None, "key_column": "CONTACT_KEY", "keys": ["CONTACT_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_CONTAINER_HIST": {"sql": None, "key_column": "CONTAINER_KEY", "keys": ["CONTAINER_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_DATASET_HIST": {"sql": None, "key_column": "DATASET_KEY", "keys": ["CONTAINER_KEY", "DATASET_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_DEADMESSAGES_HIST": {"sql": None, "key_column": "MESSAGE_KEY", "keys": ["CONTAINER_KEY", "TEMPLATE_KEY", "SEND_AFTER_MESSAGE_KEY", "DELIVERY_STATUS_KEY", "IN_REPLY_TO_MESSAGE_KEY", "COMMUNICATION_MAILING_KEY", "MESSAGE_KEY", "DATASET_ROW_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_DEADMESSAGES2_HIST": {"sql": None, "key_column": "MESSAGE_KEY", "keys": ["CONTAINER_KEY", "TEMPLATE_KEY", "SEND_AFTER_MESSAGE_KEY", "DELIVERY_STATUS_KEY", "IN_REPLY_TO_MESSAGE_KEY", "COMMUNICATION_MAILING_KEY", "MESSAGE_KEY", "DATASET_ROW_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_LOG_HIST": {"sql": None, "key_column": "LOG_KEY", "keys": ["CONTAINER_KEY", "PACKAGE_KEY", "TEMPLATE_KEY", "CAMPAIGN_KEY", "COMMUNICATION_KEY", "MESSAGE_KEY", "LOG_KEY", "RULE_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_MESSAGEDELIVERYSTATUS_HIST": {"sql": None, "key_column": "MESSAGE_DELIVERY_STATUS_KEY", "keys": ["MESSAGE_KEY", "MESSAGE_DELIVERY_STATUS_KEY", "DELIVERY_STATUS_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_MESSAGESTATUSQUEUE_HIST": {"sql": None, "key_column": "MESSAGE_STATUS_QUEUE_KEY", "keys": ["MESSAGE_STATUS_QUEUE_KEY", "CONTAINER_KEY"], "table_filtered_by": "ENTERED_AT"}, 
    "CREO_PACKAGE_HIST": {"sql": None, "key_column": "PACKAGE_KEY", "keys": ["PACKAGE_KEY", "CONTAINER_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_RULE_HIST": {"sql": None, "key_column": "RULE_KEY", "keys": ["CONTAINER_KEY", "RULE_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_TEMPLATE_HIST": {"sql": None, "key_column": "TEMPLATE_KEY", "keys": ["TEMPLATE_KEY", "BASE_TEMPLATE_KEY"], "table_filtered_by": "DATE_ENTERED",  "copy_into": "COPY_CREO_TEMPLATE.sql"}, 
    "CREO_USER_HIST": {"sql": None, "key_column": "USER_KEY", "keys": ["USER_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    "CREO_WEBHOOK_HIST": {"sql": None, "key_column": "WEBHOOK_KEY", "keys": ["CONTAINER_KEY", "WEBHOOK_KEY"], "table_filtered_by": "DATE_ENTERED"}, 
    # << #2 Incremental load using Date_Entered

    ## >> #3 Incremental load using key
    "CREO_APPROVALREQUESTITEM_HIST": {"sql": None, "key_column": "APPROVAL_REQUEST_ITEM_KEY", "keys": ["TEMPLATE_KEY", "APPROVAL_REQUEST_KEY", "APPROVAL_REQUEST_ITEM_KEY"], "table_filtered_by": None}, 
    "CREO_CAMPAIGNTYPE_HIST": {"sql": None, "key_column": "CAMPAIGN_TYPE_KEY", "keys": ["CAMPAIGN_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_CONFIG_HIST": {"sql": None, "key_column": "CONFIG_KEY", "keys": ["CONFIG_KEY"], "table_filtered_by": None}, 
    "CREO_CONTACTTYPE_HIST": {"sql": None, "key_column": "CONTACT_TYPE_KEY", "keys": ["CONTACT_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_DATASETCOLUMN_HIST": {"sql": None, "key_column": "DATASET_COLUMN_KEY", "keys": ["DATASET_COLUMN_KEY"], "table_filtered_by": None}, 
    "CREO_DATASETROW_HIST": {"sql": None, "key_column": "DATASET_ROW_KEY", "keys": ["DATASET_ROW_KEY", "DATASET_KEY"], "table_filtered_by": None}, 
    # "CREO_DATASETVALUE_HIST": { "sql": None,  "key_column": "DATASET_VALUE_KEY",  "keys": [ "DATASET_VALUE_KEY" ],  "table_filtered_by": None }, 
    "CREO_DATASOURCE_HIST": {"sql": None, "key_column": "DATASOURCE_KEY", "keys": ["DATASOURCE_KEY"], "table_filtered_by": None}, 
    "CREO_DELIVERYSTATUS_HIST": {"sql": None, "key_column": "DELIVERY_STATUS_KEY", "keys": ["DELIVERY_STATUS_KEY"], "table_filtered_by": None}, 
    "CREO_EMOJI_HIST": {"sql": None, "key_column": "EMOJI_KEY", "keys": ["EMOJI_KEY"], "table_filtered_by": None}, 
    "CREO_FOLDER_HIST": {"sql": None, "key_column": "FOLDER_KEY", "keys": ["CONTAINER_KEY", "PARENT_FOLDER_KEY", "FOLDER_KEY"], "table_filtered_by": None}, 
    "CREO_FOLDERMESSAGE_HIST": {"sql": None, "key_column": "MESSAGE_KEY", "keys": ["MESSAGE_KEY", "FOLDER_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGECONTACT_HIST": {"sql": None, "key_column": "MESSAGE_CONTACT_KEY", "keys": ["CONTACT_KEY", "MESSAGE_KEY", "MESSAGE_CONTACT_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGECONTACTTYPE_HIST": {"sql": None, "key_column": "MESSAGE_CONTACT_TYPE_KEY", "keys": ["MESSAGE_CONTACT_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGECONTACTV2_HIST": {"sql": None, "key_column": "MESSAGE_CONTACT_KEY", "keys": ["CONTACT_KEY", "MESSAGE_KEY", "MESSAGE_CONTACT_KEY"], "table_filtered_by": None}, 
    "CREO_MESSAGEPART_HIST": {"sql": None, "key_column": "MESSAGE_PART_KEY", "keys": ["MESSAGE_PART_KEY", "MESSAGE_KEY"], "table_filtered_by": None,  "copy_into": "COPY_CREO_MESSAGEPART.sql"}, 
    # "CREO_MESSAGEPARTV2_HIST": { "sql": None,  "key_column": "MESSAGE_PART_KEY",  "keys": [ "MESSAGE_PART_KEY",  "MESSAGE_KEY" ],  "table_filtered_by": None ,  "copy_into": "COPY_CREO_MESSAGEPART.sql"}, 
    "CREO_MESSAGETYPE_HIST": {"sql": None, "key_column": "MESSAGE_TYPE_KEY", "keys": ["MESSAGE_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_PARAMETER_HIST": {"sql": None, "key_column": "PARAMETER_KEY", "keys": ["PARAMETER_KEY", "DATASOURCE_KEY"], "table_filtered_by": None}, 
    "CREO_TEMPLATETYPE_HIST": {"sql": None, "key_column": "TEMPLATE_TYPE_KEY", "keys": ["TEMPLATE_TYPE_KEY"], "table_filtered_by": None}, 
    "CREO_TEMPMESSAGE_HIST": {"sql": None, "key_column": "TEMP_MESSAGE_KEY", "keys": ["TEMP_MESSAGE_KEY"], "table_filtered_by": None}, 
    # \/ TABLES WITH COMPOSITE PRIMARY KEYS \/
    "CREO_DATASETCELL_HIST": {"sql": None, "key_column": "DATASET_VALUE_KEY", "keys": ["DATASET_VALUE_KEY", "DATASET_ROW_KEY", "DATASET_COLUMN_KEY"], "table_filtered_by": None}, 
    "CREO_FOLDERCONTACT_HIST": {"sql": None, "key_column": "CONTACT_KEY", "keys": ["CONTACT_KEY", "FOLDER_KEY"], "table_filtered_by": None}, 
    "CREO_PACKAGETEMPLATE_HIST": {"sql": None, "key_column": "PACKAGE_KEY", "keys": ["PACKAGE_KEY", "TEMPLATE_KEY"], "table_filtered_by": None}, 
    "CREO_TEMPLATERULE_HIST": {"sql": None, "key_column": "TEMPLATE_KEY", "keys": ["TEMPLATE_KEY", "RULE_KEY"], "table_filtered_by": None}, 
    ## << #3 Incremental load using key

    ## >> #4 Full Loads
    "CREO_GLOBAL_HIST": {"sql": None, "key_column": None, "keys": ["APP_VERSION"], "table_filtered_by": None}, 
    ## << #4 Full Loads 
}

soda_dir="/usr/local/airflow/include/soda"

# Data driven scheduling datasets 
staging_dataset_file = "include/datasets/Staging_CREO_Dataset.txt"
staging_dataset_obj = Dataset(staging_dataset_file)

dq_dataset_file = "include/datasets/DQ_CREO_Dataset.txt"
dq_dataset_obj = Dataset(dq_dataset_file)

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

mssql_hook = MsSqlHook(mssql_conn_id="awsmssql_creosql_creo_conn")
sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

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
    # Sets the values/parameters for the config file and the checks file. 
    # Single checks file will contain checks for all tables
    def set_config(ds = None, **kwargs):
        with open(f"{soda_dir}/config/configuration.yml", "r") as config_file:
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
        with open(f"{soda_dir}/config/configuration_parametrized_CREO.yml","w") as config_file_updated:
            config_file_updated.write(conf_updated)
            logging.info("Updated the config file")

        today = datetime.strptime(ds, "%Y-%m-%d")
        logging.info(today)
        asOfDate = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        logging.info(asOfDate)
        
        # Iterate through the table list
        for table_name, table_params in tableList.items():
            logging.info(f"Retrieved table parameters for {table_name}: {table_params}")
            
            # Get the filtering column for date and the truncated table name
            date_column = table_params.get('table_filtered_by')
            trunc_table_name = table_name.replace('CREO_','').replace('_HIST','')

            # Check if a single key column or composite key columns are used
            key_column = table_params.get('key_column')
            if key_column:
                key = key_column
                order_by_clause = key
                key_columns = [key_column] # Convert to list for consistent handling

            else:
                key_columns = table_params.get('keys')
                key = key_columns[0] # Use the first key as representative
                order_by_clause = ', '.join(key_columns)

            logging.info(f"The key to order by for this table is {order_by_clause}")
            
            # Formulate Snowflake query to get sorted data
            # Use sorting to make sure there's consistent key range retrieval
            snowflake_query = f"""
                SELECT {key} FROM STG.{table_name}
                WHERE asOfDate < '{asOfDate}'
                ORDER BY {order_by_clause}
            """
            sorted_data_df = sf_hook.get_pandas_df(snowflake_query)
            
            if sorted_data_df.empty:
                raise ValueError(f"Sorted dataframe is empty")
            
            # Extract minimum and maximum key values
            min_key_values = sorted_data_df.iloc[0][key_columns]
            max_key_values = sorted_data_df.iloc[-1][key_columns]
            logging.info(f"Minimum key values: {min_key_values}")
            logging.info(f"Maximum key values: {max_key_values}")

            # Construct key range conditions for MSSQL query
            min_key_range = ' AND '.join([f"[{col}] >= {value}" for col, value in zip(key_columns, min_key_values)])
            max_key_range = ' AND '.join([f"[{col}] <= {value}" for col, value in zip(key_columns, max_key_values)])
            logging.info(f"Minimum key range: {min_key_range}")
            logging.info(f"Maximum key range: {max_key_range}")

            df = pd.DataFrame()

            # Construct MSSQL query based on whether there's a date_column
            # Sorting is used to maintain data consistency in the result
            if date_column:
                mssql_query = f"""
                    SELECT COUNT(*) FROM [dbo].[{trunc_table_name}]
                    WHERE CAST([{date_column}] AS DATE) = '{asOfDate}'
                    AND {min_key_range} AND {max_key_range}
                    -- ORDER BY {order_by_clause};
                """
            else:
                mssql_query = f"""
                    SELECT COUNT(*) FROM [dbo].[{trunc_table_name}]
                    WHERE {min_key_range} AND {max_key_range}
                    -- ORDER BY {order_by_clause};
                """

            logging.info(f"MSSQL query: \n{mssql_query}")

            df = mssql_hook.get_pandas_df(sql=mssql_query)
            if df.empty:
                raise ValueError(f"!!! No query results found // Dataframe is empty !!!")

            count = df.iloc[0, 0]
            count = 0 if count is None else count
            logging.info(f"<< Expected row count = {count} >>")

            custom_dq_checks = f"""
            checks for {table_name}:
            - missing_count(ASOFDATE) = 0
            - missing_count(METADATAFILENAME) = 0
            - record_count_differential < 10:
                record_count_differential query: |
                    SELECT {count} - COUNT(*)
                    FROM STG.{table_name}
                    WHERE ASOFDATE = to_date('{asOfDate}');
            """
            logging.info(f"Formatted check YAML file: \n{custom_dq_checks}")

            with open(f"{soda_dir}/checks/checks_CREO.yml", "a") as db_check_file:
                db_check_file.write(custom_dq_checks)
                logging.info(f"Writing CREO check file to {soda_dir}/checks/checks_CREO.yml")


        # # Get the first and last rows to determine the range
        # if not sorted_data_df.empty:
        #     min_key_values = sorted_data_df.loc[0, keys]
        #     max_key_values = sorted_data_df.loc[sorted_data_df.shape[0] - 1, keys]

        #     # Construct the concatenated keys for use in the MSSQL query
        #     min_key_concatenated = ', '.join([f"'{value}'" for value in min_key_values])
        #     max_key_concatenated = ', '.join([f"'{value}'" for value in max_key_values])

            
        #     df = pd.DataFrame()

        #     if date_column:
        #         mssql_query = f"""
        #             SELECT COUNT(*) FROM [dbo].[{trunc_table_name}]
        #             WHERE CAST([{date_column}] AS DATE) = '{asOfDate}'
        #             AND ({key_columns}) >= ({min_key_concatenated})
        #             AND ({key_columns}) <= ({max_key_concatenated})
        #             ORDER BY {key_columns};
        #         """

        #     else:
        #         mssql_query = f"""
        #             SELECT COUNT(*) FROM [dbo].[{trunc_table_name}]
        #             WHERE ({key_columns}) >= ({min_key_concatenated})
        #             AND ({key_columns}) <= ({max_key_concatenated})
        #             ORDER BY {key_columns};
        #         """

        #     logging.info(f"MSSQL query: \n{mssql_query}")

        #     df = mssql_hook.get_pandas_df(sql=mssql_query)
        #     if df.empty:
        #         raise ValueError(f"!!! No query results found // Dataframe is empty !!!")
            
        #     count = df.iloc[0,0]
        #     count = 0 if count == None else count
        #     logging.info(f"<< Expected row count = {count} >>")
            
        #     custom_dq_checks = f"""
        #     checks for {table_name}:
        #     - missing_count(ASOFDATE) = 0
        #     - missing_count(METADATAFILENAME) = 0
        #     - record_count_differential < 10:
        #         record_count_differential query: |
        #             SELECT {count} - COUNT(*) 
        #             FROM STG.{table_name}
        #             WHERE ASOFDATE = to_date('{asOfDate}');
        #     """
        #     logging.info(f"Formatted check YAML file: \n{custom_dq_checks}")
            
        #     with open(f"{soda_dir}/checks/checks_CREO.yml","a") as db_check_file:
        #         db_check_file.write(custom_dq_checks)
        #         logging.info(f"Writing CREO check file to {soda_dir}/checks/checks_CREO.yml")

    #Safety check to make sure that both the configuration file and the checks files exists and has been created successfully
    def check_config_exists(**kwargs):
        checks_file_name = f"{soda_dir}/checks/checks_CREO.yml"
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
        retries=3,
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
        bash_command=f"soda scan -d ds -c {soda_dir}/config/configuration_parametrized_CREO.yml {soda_dir}/checks/checks_CREO.yml"
    )

    @task(outlets=[dq_dataset_obj])
    def write_to_dataset():
        dataset_path = Path(dq_dataset_file)
        dataset_path.touch(exist_ok=True)
        with open(dq_dataset_file, "a") as f:
            f.write("CREO DQ complete")
            logging.info("Updated the dataset file")

start >> remove_checks >> create_checks >> set_conf >> safety_check >> soda_scan >> write_to_dataset() >> end
