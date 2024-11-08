"""
### Load CREO data to Snowflake 
Scheduled to run every morning at 6AM EST
This DAG copies data for the below listed tables based on table type (increamental, full-load) to Snowflake.

Nomenclature Cheat Sheet:
- Variables that are in ALL CAPS are global variables defined at the beginning of the script
- Directories = folder location
- Filename = name of a file
- Path = full path to a file, with the file name included
"""

import os
import logging
from datetime import datetime, timedelta
import pendulum
from pathlib import Path

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions


# - - - - - - - - - SETTINGS - - - - - - - - -

author = 'Julie Scherer'

SQL_DIR = f"/usr/local/airflow/include/sql/Staging_VC"

# Data driven scheduling datasets 
dataset_file = "include/datasets/Staging_VC_Dataset.txt"
dataset_obj = Dataset(dataset_file)

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="mssql_ictvcsql_winchkictvc_conn")
    return MSSQL_HOOK

def get_s3_hook():
    """
    Returns a S3 hook for interacting with S3.
    """
    S3_HOOK = S3Hook(aws_conn_id="aws_s3_conn")
    return S3_HOOK

def get_sf_hook():
    """
    Returns a Snowflake hook for interacting with Snowflake.
    """
    SF_HOOK = SnowflakeHook(snowflake_conn_id="snowflake_default")
    return SF_HOOK


# - - - - - - SNOWFLAKE UTILS TABLE - - - - - - -

def get_table_utils(table_name):
    utils_query = f"""
            SELECT * FROM ARES.ETL.VC_UTILS
            WHERE table_name = 'VC_{table_name}_HIST'
        """
    table_utils = get_sf_hook().get_first(utils_query)
    if not table_utils:
        raise ValueError(f"Table utilities not found. Check ETL table in Snowflake and function to retrieve utils in DAG.")
    
    return table_utils

# - - - - - - - - STAGING DAG - - - - - - - - -

## Default DAG args
default_args = {
    'owner': author,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
    "on_failure_callback": ms_teams_callback_functions.failure_callback
}

## Initialize the DAG
@dag(
    "Staging_VC",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    schedule="0 6 * * *",  # 6AM EST
    catchup=False,
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath=f"include/sql/Staging_VC/",
    default_args=default_args
)
def Staging_DAG():

    @task(multiple_outputs=True)
    def get_runtime_params(table_name, ds=None) -> dict:
        """
        Fetches runtime parameters and sets them in the context dictionary.
        Retrieves the MSSQL query based on runtime parameters and data configurations.
        The query is determined based on various conditions, including the presence
        of an SQL file, a date column, or a key column.

        Parameters:
            table_name (str): The name of the Snowflake table to export data to.
            ds (str, optional): The date string in the format "YYYY-MM-DD". Defaults to None.

        Runtime parameters:
            - table_name (str): The table name without database prefix and "_HIST".
            - columns (str): The columns to be copied.
            - aod (datetime): The date as a datetime object.
            - s3_dir_path (str): The S3 bucket file path where the data will be stored.
            - csv_file_name (str): The name of the CSV file to be created.
            - zip_file_name (str): The name of the ZIP file to be created.
            - table_utils (dict): 
        """

        query_output = get_sf_hook().get_first(f"""SELECT ETL.COPYSELECT('STG','VC_{table_name}_HIST',3)""")
        columns = query_output[f"ETL.COPYSELECT('STG','VC_{table_name}_HIST',3)"]
        aod = datetime.strptime(ds, "%Y-%m-%d")
        s3_bucket_name = Variable.get("s3_etldata_bucket_var")
        s3_dir_path = aod.strftime(f"VC/%Y/%m/%d/{table_name}")

        file_name = aod.strftime(f"{table_name}_%Y%m%d")
        csv_file_name = file_name + '.csv'
        zip_file_name = file_name + '.csv.gz'

        table_utils = get_table_utils(table_name)
        logging.info(f"Retrieved table utilities: \n{table_utils}, {type(table_utils)}")

        runtime_params = {
            "table_name": table_name,
            "columns": columns,
            "aod": aod,
            "s3_bucket_name": s3_bucket_name,
            "s3_dir_path": s3_dir_path,
            "file_name": file_name,
            "csv_file_name": csv_file_name,
            "zip_file_name": zip_file_name,
            "table_utils": table_utils,
        }
        logging.info(f"Runtime Parameters: \n\n{runtime_params}\n")

        return runtime_params


    @task
    def reset_snowflake(runtime_params):
        """
        Resets the data in the specified Snowflake table based on the provided parameters.
        """
        logging.info(f"Resetting Snowflake table")

        table_name = runtime_params.get("table_name")
        s3_dir_path = runtime_params.get("s3_dir_path")
        zip_file_name = runtime_params.get("zip_file_name")
        aod = runtime_params.get("aod")

        table_utils = runtime_params.get("table_utils")
        sql_file = table_utils.get('SQL')
        table_filtered_by = table_utils.get('TABLE_FILTERED_BY')
        key_column = table_utils.get('KEY_COLUMN')

        sfsql_query = '' # assign to empty string for error catching later

        if sql_file and sql_file != 'NULL':
            sfsql_query = f"""
                DELETE FROM STG.VC_{table_name}_HIST 
                WHERE CAST(DATE_ENTERED AS DATE) = TO_DATE('{aod}');
            """

        if table_filtered_by and table_filtered_by != 'NULL':
            sfsql_query = f"""
                DELETE FROM STG.VC_{table_name}_HIST 
                WHERE ASOFDATE = TO_DATE('{aod}');
            """

        elif key_column and key_column != 'NULL':
            s3_path = f"{s3_dir_path}/{zip_file_name}"
            sfsql_query = f"""
                    DELETE FROM STG.VC_{table_name}_HIST 
                    WHERE METADATAFILENAME = 'inbound/{s3_path}';
                """

        else:
            sfsql_query = f"""
                DELETE FROM STG.VC_{table_name}_HIST 
                WHERE ASOFDATE = TO_DATE('{aod}');
            """

        logging.info(f"Snowflake SQL query: \n{sfsql_query}")
        if not sfsql_query:
            raise ValueError(f"No Snowflake SQL query generated. Check the reset snowflake task for issues.")
        
        get_sf_hook().run(sfsql_query)
        logging.info(f"Successfully executed Snowflake query")


    @task
    def mssql_to_s3(runtime_params):
        """
        Loads the ZIP file obtained from the previous task to the specified S3 bucket.
        """
        file_name = runtime_params.get("file_name")
        s3_bucket_name = runtime_params.get("s3_bucket_name")
        s3_dir_path = runtime_params.get("s3_dir_path")
        aod = runtime_params.get("aod")

        ## Get the MSSQL query based on the runtime parameters
        logging.info(f"Retrieving MSSQL query")
        table_utils = runtime_params.get("table_utils")
        sql_file = table_utils.get('SQL')
        table_filtered_by = table_utils.get('TABLE_FILTERED_BY')
        key_column = table_utils.get('KEY_COLUMN')
        
        ## Assign the query to empty string for error catching later
        mssql_query = '' 
        
        # If there's a custom SQL file...
        if sql_file and sql_file != 'NULL':
            # mssql_query = use_sql_file(aod=aod, sql_file=sql_file)
            with open(f"{SQL_DIR}/{sql_file}", "r") as file:
                mssql_query = file.read().format(aod)
        
        # If there's a date column to filter by...
        if table_filtered_by and table_filtered_by != 'NULL':
            mssql_query = f"""
                SELECT * FROM [dbo].[{table_name}]
                WHERE CAST([{table_filtered_by}] AS DATE) = '{aod}';
            """
        
        # If there's a key column to filter by...
        elif key_column and key_column != 'NULL':
            snowflake_query = f"""
                SELECT MAX({key_column})
                FROM STG.VC_{table_name}_HIST
            """
            max_value_df = get_sf_hook().get_pandas_df(snowflake_query)
            max_value = max_value_df.iloc[0, 0]
            max_value = 0 if max_value is None else max_value
            mssql_query = f"""
                SELECT * 
                FROM [dbo].[{table_name}]
                WHERE [{key_column}] > {max_value};
            """
        
        # If none of the above are true...
        else:
            mssql_query = f"SELECT * FROM [dbo].[{table_name}]"

        logging.info(f"Microsoft SQL Server query: \n{mssql_query}")
        if not mssql_query:
            raise ValueError(f"No Microsoft SQL query generated. Check the BCP task for issues.")

        # # - - - - - - - - - - - - - - - 
        chunks = get_mssql_hook().get_pandas_df_by_chunks(sql=mssql_query, chunksize=20000)
        for idx, chunk in enumerate(chunks):
            df_byte = chunk\
                .to_csv(
                    header=False,  # Exclude the column headers from the CSV
                    index=False,  # Exclude the row index from the CSV
                    sep='|',  # Use the pipe symbol as the column separator
                    na_rep='NULL',  # Replace missing values with 'NULL'
                    compression='gzip',  # Compress the CSV file using gzip
                    doublequote=True,  # Enable double quoting for values
                    quotechar='"',
                ).encode()
            # Load chunk to S3
            get_s3_hook().load_bytes(
                bytes_data=df_byte,
                bucket_name=s3_bucket_name,
                key=f"inbound/{s3_dir_path}/{file_name}_{idx}.czv.gz",
                replace=True,
            )
            logging.info(f'Loading {file_name}_{idx}.czv.gz to S3')


    @task
    def copy_s3_to_snowflake(runtime_params):
        """
        Copies data from the S3 bucket to the Snowflake database table specified in the runtime parameters.

        The SQL query is constructed dynamically based on the data and runtime parameters.
        The data is copied using the "COPY INTO" Snowflake SQL command.
        """
        table_name = runtime_params.get("table_name")
        columns = runtime_params.get("columns")
        previous_day = runtime_params.get("aod")
        s3_dir_path = runtime_params.get("s3_dir_path")
        zip_file_name = runtime_params.get("zip_file_name")

        sql_query = f"""
            -- \\ VC_{table_name}_HIST
            COPY INTO STG.VC_{table_name}_HIST 
            FROM (
                SELECT 
                    METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{previous_day}'), 
                    {columns} 
                FROM @ETL.INBOUND/{s3_dir_path}/
            )
            FILE_FORMAT = ( 
                FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
            ) 
            PATTERN = '.*{zip_file_name}.*'
        """
        get_sf_hook().run(sql_query)


    @task(outlets=[dataset_obj])
    def write_to_dataset():
        Path.touch(dataset_file, exist_ok=True)
        with open(dataset_file, "a") as file:
            file.write("Staging_CREO data load complete")
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    start = DummyOperator(task_id="start")
    mid = DummyOperator(task_id="mid")
    end = DummyOperator(task_id="end", trigger_rule="all_done")
    
    for table_name in ['ACH_History', 'AutoReportEditHistory', 'BalSheet2', 'BankruptcyTrustee', 'CashedCheck', 'CCardResponses', 'CollBonusPTP', 'CollectionAction', 'CollectionMovement', 'CollectionNote', 'CollectionStream', 'CreditCardAttempts', 'CreditCardBlock', 'CreditCards', 'CreditCardsEdit', 'CreditCardTrans', 'CreditReportingBaseSegment', 'CreditReportingBaseSegmentHistory', 'CreditReportingLoanActivity', 'CreditReportingLoanDisputeNote', 'CreditReportingProcessingQueue', 'CreditReportingProcessingQueueHistory', 'CreditVendorData', 'CustomerAddress', 'CustomerAddressEdit', 'CustomerEdit', 'CustomerEmployerEdit', 'CustomerNote', 'CustomerPhoneNumber', 'CustomerPhoneNumberEdit', 'DocuwareDocument', 'DocuwareID', 'DocuwareVisitorEmailAttachmentXref', 'DrawerZ', 'EndOfDayRpt', 'ExcludeFromCapsHistory', 'IssuerEdit', 'LoanChkAcctChange', 'LoanDepositStatusHistory', 'LoanDueDateChange', 'LoanNote', 'LoanPayment', 'LoanPaymentCheckPaymentTypeXref', 'LoanPaymentRefund', 'LoanPaymentStaged', 'LoanPayoffDate', 'LoanStatusChange', 'LocationUS_ZipcodesXRef', 'MPayInterest', 'MPayLoanInSyncAdj', 'MPayRecalcLoanPaymentAdj', 'OverShort', 'PaymentsPastDue', 'Presentment', 'PresentmentRequest', 'PresentmentRequestNotSent', 'PromiseToPay', 'PromiseToPayCommunication', 'PromiseToPayDetailEdit', 'Receipt', 'TellerIDEdit', 'TellerPwdHistory', 'TellerSecurity', 'TransDetail', 'TransPOS', 'VaultCount', 'VaultRecalcAdj', 'VisitorAuthenticationCode', 'VisitorAuthenticationCodeAttempt', 'VisitorDevice', 'VisitorDocument', 'VisitorEdit', 'VisitorEmail', 'VisitorPasswordHistory', 'WebPixelVendorData', 'ACH_Recv', 'ACH_Sent', 'ACHSentParent', 'BalSheet_TransDetail', 'BalSheetColumns2', 'BankAccount', 'BatchExecution', 'CapsRun', 'CollectionAgingItem', 'CreditReportingLoanDispute', 'CreditReportingLoanStatusHistory', 'CreditReportingRun', 'DepositChk', 'DepositChkDetail', 'DialerKeys', 'DocuwareLoanDoc', 'DocuwareStatus', 'DocuwareVisitorDocXRef', 'DrawerMaster', 'DrawerZCash', 'EndOfDayInventoryDetail', 'EndOfDayRptDetail', 'FormLetterBatch', 'FormLetterBatchVendorFile', 'FormLetterPrinted', 'FormLetterResult', 'GLAcct', 'Issuer', 'LoanAuthorizedPaymentMethod', 'LoanFundingHistory', 'LoanFundingHistoryDetail', 'LoanPaymentDecreaseAmountOwed', 'LoanPaymentDueDate', 'LoanPaymentMPay', 'LoanPaymentSuspendInterest', 'PaymentsPastDueDetail', 'PresentmentCreditCardTransXRef', 'PresentmentRequestACHHistoryXRef', 'PresentmentRequestNotSentReason', 'PromiseToPayDetail', 'PromiseToPayDetailTrans', 'RISREPT', 'RisReptDoNotContact', 'Store_Windows', 'TELLERID', 'TellerLogin', 'TransDetailAcct', 'TransDetailCash', 'TransDetailCashParsedCash', 'TransDetailCheck', 'TransDetailLoan', 'US_Zipcodes', 'VaultMgrAuthorization', 'VaultMgrAuthorizationDetail', 'VisitorCommunicationPreference', 'VisitorEmailDisposition', 'WebCallCenterLogin', 'WebCallQueue', 'WebCallQueueAudit', 'WebCallRARRHistory', 'WebCallUserSetting', 'WebCallWorkItemCategoryHistory', 'WebCallWorkQueue', 'GlobalHistory']: 
        with TaskGroup(group_id=f"VC_{table_name}_HIST_Task") as staging_pipeline:
    
            runtime_params = get_runtime_params(table_name, ds="2023-07-01")
            # runtime_params

            reset_snowflake_task = reset_snowflake(runtime_params)
            mssql_to_s3_task = mssql_to_s3(runtime_params)
            copy_s3_to_snowflake_task = copy_s3_to_snowflake(runtime_params)
            
            reset_snowflake_task >> mssql_to_s3_task >> copy_s3_to_snowflake_task
            
        start >> staging_pipeline >> mid
    
    mid >> write_to_dataset() >> end

staging_dag = Staging_DAG()