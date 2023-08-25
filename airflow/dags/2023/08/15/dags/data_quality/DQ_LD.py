"""
### LD DQ checks 
Triggered by.: STAGING_LD DAG.
Description..: Data Quality checks for nightly loads.
Working......: Checks are made against Source database WinchCANLD
               DAG dynamically creates checks_CM.yml for all the tables in the dictionary. And calls SODA passing checks_LD.yml. 
               Following checks are run against table dictionary (checks.yml):
                    #1 Missing(asofdate) = 0 Checks for nulls and blanks 
                    #2 Missing(filename) = 0 Checks for nulls and blanks
                    #3 duplicate_keys    = 0 Checks for a given key if tables has duplicate records ** DECOMMISSIONED **
                    #4 record_count_differential < 10 Checks for record cound difference should not be greater than 10
Table dict...: for eg: 'LD_InsuranceEnrollment_HIST':{"Key_column":'INSURANCE_ENROLLMENT_KEY', "date_field":True}'
                    #1 and #2 Missing asofdate and filename are default
                    #3 is based on table attribute
                    #4 count diff check is by default   
Upon Failure.: Sends notification alerts on Teams Channel.
"""

from datetime import datetime, timedelta
from airflow import DAG, macros, Dataset
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils import trigger_rule
from airflow.models import Variable
from include.functions import ms_teams_callback_functions
from airflow.operators.bash import BashOperator
import pendulum
import logging
import numpy as np

db_name = "awsmssql_canldsql_winchkcanld_conn"
snowflake = "snowflake_default"

# Data driven scheduling datasets 
staging_ld_dataset = "include/datasets/ds_staging_ld.txt"
ds_Staging_LD = Dataset(staging_ld_dataset)
dataset_name = "include/datasets/ds_dq_ld.txt"
ds_LD_DQ = Dataset(dataset_name)

tableList = {'LD_InsuranceEnrollment_HIST':{"Key_column":'INSURANCE_ENROLLMENT_KEY', "date_field":True}
        ,'LD_LoanFunding_HIST':{"Key_column":'LOAN_FUNDING_KEY', "date_field":True}
        ,'LD_PresentmentRequest_HIST':{"Key_column":'PRESENTMENT_REQUEST_KEY', "date_field":True}
        ,'LD_Company_HIST':{"Key_column":'LOCATION', "date_field":True}
        ,'LD_OpenEndLoanStatement_HIST':{"Key_column":'OPEN_END_LOAN_STMT_KEY', "date_field":True}
        ,'LD_TransDetail_HIST':{"Key_column":'TRANS_DETAIL_KEY', "date_field":True}
        ,'LD_OpenEndInterest_HIST':{"Key_column":'OPEN_END_INTEREST_KEY', "date_field":True}
        ,'LD_MPayInterest_HIST':{"Key_column":'MPAY_INTEREST_KEY', "date_field":True}
        ,'LD_Presentment_HIST':{"Key_column":'PRESENTMENT_KEY', "date_field":True}
        ,'LD_CustomerIncome_HIST':{"Key_column":'CUSTOMER_INCOME_KEY', "date_field":True}
        ,'LD_RBCEFundBatch_HIST':{"Key_column":'RBC_EFUND_BATCH_KEY', "date_field":True}
        ,'LD_PromiseToPay_HIST':{"Key_column":'PROMISE_TO_PAY_KEY', "date_field":True}
        ,'LD_Loan_HIST':{"Key_column":'LOAN_KEY', "date_field":False}
        ,'LD_BalSheet2_HIST':{"Key_column":'BALSHEET_KEY', "date_field":True}
        ,'LD_EndOfDayRpt_HIST':{"Key_column":'EODR_KEY', "date_field":True}
        ,'LD_CreditCardResultCode_HIST':{"Key_column":'CREDIT_CARD_RESULT_CODE_KEY', "date_field":True}
        ,'LD_LoanProductConfig_HIST':{"Key_column":'LOAN_PRODUCT_CONFIG_KEY', "date_field":True}
        ,'LD_LoanProduct_HIST':{"Key_column":'LOAN_PRODUCT_KEY', "date_field":True}
        ,'LD_LoanPayment_HIST':{"Key_column":'LOAN_PAYMENT_KEY', "date_field":True}
        ,'LD_LoanApplication_HIST':{"Key_column":'LOAN_APPLICATION_KEY', "date_field":True}
        ,'LD_SPayInterest_HIST':{"Key_column":'SPAY_INTEREST_KEY', "date_field":True}
        ,'LD_Customer_HIST':{"Key_column":'CUSTOMER_KEY', "date_field":True}
        ,'LD_CustomerAddress_HIST':{"Key_column":'CUSTOMER_ADDRESS_KEY', "date_field":True}
        ,'LD_CreditCardAttempts_HIST':{"Key_column":'CREDIT_CARD_TRANS_KEY', "date_field":True}
        ,'LD_CreditCardTrans_HIST':{"Key_column":'CREDIT_CARD_TRANS_KEY', "date_field":True}
        ,'LD_ACH_History_HIST':{"Key_column":'ACH_HISTORY_KEY', "date_field":True}
        ,'LD_ServiceTrans_HIST':{"Key_column":'SERVICE_TRANS_KEY', "date_field":True}
        ,'LD_ServiceDetail_HIST':{"Key_column":'SERVICE_DETAIL_KEY', "date_field":True}
        ,'LD_CashedCheck_HIST':{"Key_column":'CASHED_CHECK_KEY', "date_field":True}
        ,'LD_LoanIncome_HIST':{"Key_column":'LOAN_INCOME_KEY', "date_field":False}
        ,'LD_PromiseToPayDetail_HIST':{"Key_column":'PTP_DETAIL_KEY', "date_field":False}
        ,'LD_OpenEndLoan_HIST':{"Key_column":'OPEN_END_LOAN_KEY', "date_field":False}
        ,'LD_RISREPT_HIST':{"Key_column":'RISREPT_KEY', "date_field":False}
        ,'LD_ACH_Sent_HIST':{"Key_column":'ACH_SENT_KEY', "date_field":False}
        ,'LD_MPayLoan_HIST':{"Key_column":'MPAY_LOAN_KEY', "date_field":False}
        ,'LD_Transcode_HIST':{"Key_column":'TRANS_CODE_KEY', "date_field":False}
        ,'LD_CapsSkipReason_HIST':{"Key_column":'CAPS_SKIP_REASON_KEY', "date_field":False}
        ,'LD_LoanProductFinancialGroup_HIST':{"Key_column":'LOAN_PRODUCT_FINANCIAL_GROUP_KEY', "date_field":False} 
        ,'LD_LoanFundingStatus_HIST':{"Key_column":'FUNDING_STATUS_KEY', "date_field":False}
        ,'LD_PresentmentType_HIST':{"Key_column":'PRESENTMENT_TYPE_KEY', "date_field":False}
        ,'LD_District_HIST':{"Key_column":'DISTRICT_KEY', "date_field":False}
        ,'LD_Region_HIST':{"Key_column":'REGION_KEY', "date_field":False}
        ,'LD_Currency_HIST':{"Key_column":'CURRENCY_KEY', "date_field":False}
        ,'LD_Markets_HIST':{"Key_column":'MARKET_KEY', "date_field":False}   
        ,'LD_LoanPaymentMPay_HIST':{"Key_column":'LOAN_PAYMENT_MPAY_KEY', "date_field":False}
        ,'LD_MPayAmort_HIST':{"Key_column":'MPAY_AMORT_KEY', "date_field":False}
        ,'LD_RBCEFundBatchDetail_HIST':{"Key_column":'RBC_EFUND_BATCH_DETAIL_KEY', "date_field":False}
        ,'LD_PresentmentRequestACHHistoryXREF_HIST':{"Key_column":'ACH_HISTORY_KEY', "date_field":False}    
        ,'LD_ACHBank_HIST':{"Key_column":'ACH_BANK_KEY', "date_field":False}            
        ,'LD_LoanPaymentOpenEnd_HIST':{"Key_column":'LOAN_PAYMENT_OPEN_END_KEY', "date_field":False}  
        ,'LD_TRANSDETAILACCT_HIST':{"Key_column":'TRANS_DETAIL_ACCT_KEY', "date_field":False}    
        ,'LD_CreditCardVendor_HIST':{"Key_column":'CREDIT_CARD_VENDOR_KEY', "date_field":False}
        ,'LD_BalSheet_TransDetail_HIST':{"Key_column":'TRANS_DETAIL_KEY,BALSHEET_KEY,LOCATION', "date_field":False}
        ,'LD_EndOfDayInventoryDetail_HIST':{"Key_column":'EODR_KEY,LOAN_KEY', "date_field":False}
        ,'LD_OpenEndLoanStatementBalance_HIST':{"Key_column":'OPEN_END_LOAN_STMT_KEY', "date_field":False}
    }

with DAG(
    dag_id="DQ_LD",
    catchup=False,
    start_date=pendulum.datetime(2023, 5, 5, tz='US/Eastern'),
    schedule = [ds_Staging_LD],
    #schedule_interval = None, # DAG Time Zone set to EST 
    doc_md=__doc__,
    tags=["Author: Shunjian Wang","cross-DAG dependencies", "Staging_LD"],
    default_args={
        "retries": 1,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=1)
        ,"on_failure_callback": ms_teams_callback_functions.failure_callback
    }
) as dag:
    
    SODA_PATH="/usr/local/airflow/include/soda"
    
    #Sets the values/parameters for the config file and the checks file. Single checks file will contain checks for all tables
    def set_config(ds = None, **kwargs):
        config_file = open("{directory}/config/configuration.yml".format(directory = SODA_PATH),"r")
        conf = config_file.read()
        conf_updated = conf.format(username = Variable.get("soda_username_var"), password = Variable.get("soda_pwd_var"), account = Variable.get("soda_account_var"), database = Variable.get("soda_database_var"), warehouse = Variable.get("soda_warehouse_var"), role = Variable.get("soda_role_var"), schema = Variable.get("soda_schema_var"))
        config_file_updated = open("{directory}/config/configuration_parametrized_LD.yml".format(directory = SODA_PATH),"w")
        config_file_updated.write(conf_updated)
        config_file.close()
        config_file_updated.close()
        logging.info("Updated the config file")

        file = open("{directory}/checks/{filename}".format(directory = SODA_PATH, filename = "checks.yml"), "r")
        dq = file.read() 
        file.close()
        today = datetime.strptime(ds, "%Y-%m-%d")
        asOfDate = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        logging.info(today)
        logging.info(asOfDate)
        file2 = open(f"{SODA_PATH}/checks/checks_LD.yml","a")

        for table in tableList:
            key = tableList[table]["Key_column"]
            key1 = key.split(',')[0]
            logging.info("The key for this table is {}".format(key1))
            
            table_name = table
            mssql_hook = MsSqlHook(mssql_conn_id=db_name)
            sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
            df = None
            
            #Getting the minimum value for the key range for the snowflake table. As of date is set as yesterday's date so it will only include records up to the end of yesterday(But not including today)
            #Thus, all values for the key for today and onwards should be larger than this value. If table is empty, the key is treated as 0
            snowflake_query_min = "select max({column}) from STG.{table} WHERE asOfDate < '{date}'".format(column = key1, table = table, date = asOfDate)
            min_value_df = sf_hook.get_pandas_df(snowflake_query_min)
            min_value = min_value_df.iloc[0,0]
            if min_value == None:
                min_value = 0

            #Getting the maximum value for the key range. If table is empty, the key is treated as 0
            snowflake_query_max = "select max({column}) from STG.{table}".format(column = key1, table = table)
            max_value_df = sf_hook.get_pandas_df(snowflake_query_max)
            max_value = max_value_df.iloc[0,0]
            if max_value == None:
                max_value = 0
            
            if tableList[table_name]["date_field"]:
                df = mssql_hook.get_pandas_df(sql="select count(*) FROM dbo.{t} WHERE CAST(DATE_ENTERED AS DATE) = '{target_date}' AND {key} > {floor} and {key} <= {ceiling};".format(t=table_name.replace("LD_","")[:-5], target_date = asOfDate, key = key1, floor = min_value, ceiling = max_value))
            else:
                df = mssql_hook.get_pandas_df(sql="select count(*) FROM dbo.{t} WHERE {key} > {floor} and {key} <= {ceiling};".format(t=table.replace("LD_","")[:-5], key = key1, floor = min_value, ceiling = max_value))

            count = df.iloc[0,0]
            if count == None:
                count = 0
            custom_dq_checks = None
            custom_dq_checks = dq.format(table=table_name, md = asOfDate, expected = count)            
            logging.info(custom_dq_checks)
            
            file2.write(custom_dq_checks)             
        
        file2.close()

    #Safety check to make sure that both the configuration file and the checks files exists and has been created successfully
    def check_config_exists(**kwargs):
        checks_file_name = f"{SODA_PATH}/checks/checks_LD.yml"
        checks_file = open(checks_file_name,"r")
        logging.info("Printing checks contents")
        logging.info(checks_file.read())
        checks_file.close()
    
    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "end"]]
    
    #Operators for creating and cleaning up the initial checks file. Just an empty file is sufficient as it will have its values/parameters dynamically written in during the set config phase
    remove_checks = BashOperator(
        task_id='remove_checks',
        bash_command='rm -f /usr/local/airflow/include/soda/checks/checks_LD.yml',
    )

    create_checks = BashOperator(
        task_id='create_checks',
        bash_command='touch /usr/local/airflow/include/soda/checks/checks_LD.yml',
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
        task_id="dq_LD",
        bash_command=f"soda scan -d ds -c {SODA_PATH}/config/configuration_parametrized_LD.yml {SODA_PATH}/checks/checks_LD.yml"
    )

    @task(outlets=[ds_LD_DQ])
    def write_to_dataset():
        f = open(dataset_name, "a")
        f.write("ld dq complete")
        f.close()

start >> remove_checks >> create_checks >> set_conf >> safety_check >> soda_scan >> write_to_dataset() >> end 
