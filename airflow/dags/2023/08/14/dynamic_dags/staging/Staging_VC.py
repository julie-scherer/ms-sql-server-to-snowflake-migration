"""
### Load Verge Credit data to Snowflake 
Scheduled to run every morning at 6AM EST
This DAG copies data for the below listed tables based on table type (increamental, full-load) to Snowflake.
"""
from datetime import datetime, timedelta
import dateutil.relativedelta

import os
import numpy as np
import pandas as pd
import logging
from airflow import XComArg
from airflow import DAG, macros, Dataset
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain 
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import trigger_rule
from airflow.models import Variable
import pendulum
from include.functions import ms_teams_callback_functions
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

db_name = "mssql_ictvcsql_winchkictvc_conn"#The connection string for the VC source
snowflake = "snowflake_default"

staging_vc_dataset = "include/datasets/ds_staging_vc.txt"
ds_Staging_VC = Dataset(staging_vc_dataset)

fullTableList = {
    #These tables all have the date_entered field
    'VC_ACH_History_HIST':{"sql":None, "has_date_entered":True},
    'VC_AutoReportEditHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_BalSheet2_HIST':{"sql":None, "has_date_entered":True},
    'VC_BankruptcyTrustee_HIST':{"sql":None, "has_date_entered":True},
    'VC_CashedCheck_HIST':{"sql":None, "has_date_entered":True},
    'VC_CCardResponses_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollBonusPTP_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionAction_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionMovement_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionStream_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardAttempts_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardBlock_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCards_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardsEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardTrans_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingBaseSegment_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingBaseSegmentHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingLoanActivity_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingLoanDisputeNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingProcessingQueue_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingProcessingQueueHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditVendorData_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerAddress_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerAddressEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerEmployerEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerPhoneNumber_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerPhoneNumberEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_DocuwareDocument_HIST':{"sql":None, "has_date_entered":True},
    'VC_DocuwareID_HIST':{"sql":None, "has_date_entered":True},
    'VC_DocuwareVisitorEmailAttachmentXref_HIST':{"sql":None, "has_date_entered":True},
    'VC_DrawerZ_HIST':{"sql":None, "has_date_entered":True},
    'VC_EndOfDayRpt_HIST':{"sql":None, "has_date_entered":True},
    'VC_ExcludeFromCapsHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_IssuerEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanChkAcctChange_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanDepositStatusHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanDueDateChange_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPayment_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPaymentCheckPaymentTypeXref_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPaymentRefund_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPaymentStaged_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPayoffDate_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanStatusChange_HIST':{"sql":None, "has_date_entered":True},
    'VC_LocationUS_ZipcodesXRef_HIST':{"sql":None, "has_date_entered":True},
    'VC_MPayInterest_HIST':{"sql":None, "has_date_entered":True},
    'VC_MPayLoanInSyncAdj_HIST':{"sql":None, "has_date_entered":True},
    'VC_MPayRecalcLoanPaymentAdj_HIST':{"sql":None, "has_date_entered":True},
    'VC_OverShort_HIST':{"sql":None, "has_date_entered":True},
    'VC_PaymentsPastDue_HIST':{"sql":None, "has_date_entered":True},
    'VC_Presentment_HIST':{"sql":None, "has_date_entered":True},
    'VC_PresentmentRequest_HIST':{"sql":None, "has_date_entered":True},
    'VC_PresentmentRequestNotSent_HIST':{"sql":None, "has_date_entered":True},
    'VC_PromiseToPay_HIST':{"sql":None, "has_date_entered":True},
    'VC_PromiseToPayCommunication_HIST':{"sql":None, "has_date_entered":True},
    'VC_PromiseToPayDetailEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_Receipt_HIST':{"sql":None, "has_date_entered":True},
    'VC_TellerIDEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_TellerPwdHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_TellerSecurity_HIST':{"sql":None, "has_date_entered":True},
    'VC_TransDetail_HIST':{"sql":None, "has_date_entered":True},
    'VC_TransPOS_HIST':{"sql":None, "has_date_entered":True},
    'VC_VaultCount_HIST':{"sql":None, "has_date_entered":True},
    'VC_VaultRecalcAdj_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorAuthenticationCode_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorAuthenticationCodeAttempt_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorDevice_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorDocument_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorEmail_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorPasswordHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_WebPixelVendorData_HIST':{"sql":None, "has_date_entered":True},

    #These tables do not have the date_entered field and must be loaded using another strategy
    'VC_ACH_Recv_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_RECV_KEY"},
    'VC_ACH_Sent_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_SENT_KEY"},
    'VC_ACHSentParent_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_SENT_PARENT_KEY"},
    'VC_BalSheet_TransDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"BALSHEET_KEY"},
    'VC_BalSheetColumns2_HIST':{"sql":None, "has_date_entered":False, "key_column":"BSC_KEY"},
    'VC_BankAccount_HIST':{"sql":None, "has_date_entered":False, "key_column":"Bank_account_KEY"},
    'VC_BatchExecution_HIST':{"sql":None, "has_date_entered":False, "key_column":"Batch_EXECUTION_KEY"},
    'VC_CapsRun_HIST':{"sql":None, "has_date_entered":False, "key_column":"CAPS_RUN_KEY"},
    'VC_CollectionAgingItem_HIST':{"sql":None, "has_date_entered":False, "key_column":"COLLECTION_AGING_CONFIG_DAYS_KEY"},
    'VC_CreditReportingLoanDispute_HIST':{"sql":None, "has_date_entered":False, "key_column":"CREDIT_REPORTING_LOAN_DISPUTE_KEY"},
    'VC_CreditReportingLoanStatusHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_KEY"},
    'VC_CreditReportingRun_HIST':{"sql":None, "has_date_entered":False, "key_column":"CREDIT_REPORTING_RUN_KEY"},
    'VC_DepositChk_HIST':{"sql":None, "has_date_entered":False, "key_column":"DEPOSIT_CHK_KEY"},
    'VC_DepositChkDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"DEPOSIT_CHK_DETAIL_KEY"},
    'VC_DialerKeys_HIST':{"sql":None, "has_date_entered":False, "key_column":"DIALER_KEYS_KEY"},
    'VC_DocuwareLoanDoc_HIST':{"sql":None, "has_date_entered":False, "key_column":"DOCUWARE_LOAN_DOC_KEY"},
    'VC_DocuwareStatus_HIST':{"sql":None, "has_date_entered":False, "key_column":"DocuwareStatus_KEY"},
    'VC_DocuwareVisitorDocXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":"VISITOR_DOCUMENT_KEY"},
    'VC_DrawerMaster_HIST':{"sql":None, "has_date_entered":False, "key_column":"Drawer_key"},
    'VC_DrawerZCash_HIST':{"sql":None, "has_date_entered":False, "key_column":"DRAWERZ_CASH_KEY"},
    'VC_EndOfDayInventoryDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_KEY"},
    'VC_EndOfDayRptDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_DET_KEY"},
    'VC_FormLetterBatch_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_BATCH_KEY"},
    'VC_FormLetterBatchVendorFile_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_BATCH_BUILD_VENDOR_FILE_KEY"},
    'VC_FormLetterPrinted_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_PRINTED_KEY"},
    'VC_FormLetterResult_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_RESULT_KEY"},
    'VC_GLAcct_HIST':{"sql":None, "has_date_entered":False, "key_column":"GL_ACCT_KEY"},   
    'VC_Issuer_HIST':{"sql":None, "has_date_entered":False, "key_column":"ISSUER_KEY"},
    'VC_LoanAuthorizedPaymentMethod_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_AUTHORIZED_PAYMENT_METHOD_KEY"},
    'VC_LoanFundingHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_FUNDING_HISTORY_KEY"},
    'VC_LoanFundingHistoryDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_FUNDING_HISTORY_DETAIL_KEY"},
    'VC_LoanPaymentDecreaseAmountOwed_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_DECREASE_AMOUNT_OWED_KEY"},
    'VC_LoanPaymentDueDate_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_DUE_DATE_KEY"},
    'VC_LoanPaymentMPay_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_MPAY_KEY"},
    'VC_LoanPaymentSuspendInterest_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_SUSPEND_INTEREST_KEY"},
    'VC_PaymentsPastDueDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"PAYMENTS_PAST_DUE_DETAIL_KEY"},
    'VC_PresentmentCreditCardTransXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":"CREDIT_CARD_TRANS_KEY"},
    'VC_PresentmentRequestACHHistoryXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_HISTORY_KEY"},
    'VC_PresentmentRequestNotSentReason_HIST':{"sql":None, "has_date_entered":False, "key_column":"PRESENTMENT_REQUEST_NOT_SENT_REASON_KEY"},
    'VC_PromiseToPayDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"PTP_DETAIL_KEY"},
    'VC_PromiseToPayDetailTrans_HIST':{"sql":None, "has_date_entered":False, "key_column":"PTP_DETAIL_TRANS_KEY"},
    'VC_RISREPT_HIST':{"sql":None, "has_date_entered":False, "key_column":"RISREPT_KEY"},
    'VC_RisReptDoNotContact_HIST':{"sql":None, "has_date_entered":False, "key_column":"RIS_REPT_DO_NOT_CONTACT_KEY"},
    'VC_Store_Windows_HIST':{"sql":None, "has_date_entered":False, "key_column":"Store_Windows_Key"},
    'VC_TELLERID_HIST':{"sql":None, "has_date_entered":False, "key_column":"TELLER_ID_KEY"},
    'VC_TellerLogin_HIST':{"sql":None, "has_date_entered":False, "key_column":"TELLER_LOGIN_KEY"},
    'VC_TransDetailAcct_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"},
    'VC_TransDetailCash_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_CASH_KEY"},
    'VC_TransDetailCashParsedCash_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_CASH_PARSED_CASH_KEY"},
    'VC_TransDetailCheck_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_CHECK_KEY"},
    'VC_TransDetailLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_LOAN_KEY"},
    'VC_US_Zipcodes_HIST':{"sql":None, "has_date_entered":False, "key_column":"ZIPCODE"},
    'VC_VaultMgrAuthorization_HIST':{"sql":None, "has_date_entered":False, "key_column":"VM_AUTH_KEY"},
    'VC_VaultMgrAuthorizationDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"VM_AUTH_DETAIL_KEY"},
    'VC_VisitorCommunicationPreference_HIST':{"sql":None, "has_date_entered":False, "key_column":"VISITOR_COMMUNICATION_PREFERENCE_KEY"},
    'VC_VisitorEmailDisposition_HIST':{"sql":None, "has_date_entered":False, "key_column":"VISITOR_EMAIL_DISPOSITION_KEY"},
    'VC_WebCallCenterLogin_HIST':{"sql":None, "has_date_entered":False, "key_column":"CallCenter_Login_Key"},
    'VC_WebCallQueue_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_QUEUE_KEY"},
    'VC_WebCallQueueAudit_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_QUEUE_AUDIT_KEY"},
    'VC_WebCallRARRHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_RARR_HISTORY_KEY"},
    'VC_WebCallUserSetting_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_USER_SETTING_KEY"},
    'VC_WebCallWorkItemCategoryHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_WORK_ITEM_CATEGORY_HISTORY_KEY"},
    'VC_WebCallWorkQueue_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_WORK_QUEUE_KEY"},

    #These tables will be loaded via a full snapshot for every day with an additional view created from the table where only the most recent days worth of data is included
    'VC_GlobalHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
}

@dag(
    "Staging_VC",
    start_date=pendulum.datetime(2023, 8, 1, tz='US/Eastern'),
    catchup=False,
    #catchup=True,
    schedule_interval = '0 6 * * *', #  6AM EST
    dagrun_timeout = timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath="include/sql/Staging_VC/",
    tags=["Author: Shunjian Wang"],
    default_args={
        "retries": 0,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        'execution_timeout': timedelta(hours=3),
        "on_failure_callback": ms_teams_callback_functions.failure_callback
    })

def staging_VC():
    start, mid, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "mid", "end"]]

    @task(multiple_outputs=True)
    def get_run_param(table_name,ds=None,ti=None) -> dict:
        sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
        output = sf_hook.get_first("SELECT ETL.COPYSELECT('STG','{table}',3)".format(table=table_name.upper()))
        result = output["ETL.COPYSELECT('STG','{table}',3)".format(table=table_name.upper())]
        aod = datetime.strptime(ds, "%Y-%m-%d") 

        pattern = "VC/{year}/{month}/{day}/{table}/".format(year=aod.strftime("%Y"), month=aod.strftime("%m"), day=aod.strftime("%d"), table=table_name.replace("VC_","")[:-5])
        target_file = "{table_t}_{date}.csv.gz".format(table_t = table_name.replace("VC_","")[:-5], date=aod.strftime('%Y%m%d'))
        return {
                "columns": result,              # get_columns
                "s3_file_path": pattern,        # The file path on the S3 location
                "asOfDate": aod,                # _asofdate
                "table": table_name,            # .*tablename_AsOfDate.csv.gz.*
                "file_pattern":target_file
            }
    
    @task()
    def copy_mssql_to_s3(table_data: dict, table):
        
        # log get_run_params 
        logging.info(f"Table_data: {table_data}")
        
        #Defining all hooks
        mssql_hook = MsSqlHook(mssql_conn_id=db_name)
        s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
        sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
        df = pd.DataFrame()  # camel casing is IMPORTANANDOO!
        s3_bucket= Variable.get("s3_etldata_bucket_var")
        
        ## << #1 Custom SQL >>
        if fullTableList[table]["sql"] != None:
            logging.info(f"<< #1 Running for Custom SQL: {table} >>".format(table=table_data["table"]))
            
            #Resetting table on snowflake
            sf_sql = "delete from STG.{table} where cast(date_entered as date) = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
            sf_hook.run(sf_sql)

            #Getting the sql query
            sql_get_data = fullTableList[table]["sql"]
            dir = "/usr/local/airflow/include/sql/Staging_VC"
            file = open("{directory}/{filename}".format(directory = dir, filename = sql_get_data), "r")
            mssql_query = file.read()
            logging.info(mssql_query.format(asOfDate=table_data["asOfDate"]))
            
            #Getting the MSSQL data  
            df = mssql_hook.get_pandas_df(sql=mssql_query.format(asOfDate=table_data["asOfDate"]))           
        
        ## << #2 Incremental load using DATE_ENTERED >>
        elif fullTableList[table]["sql"] == None and fullTableList[table]["has_date_entered"]:
            logging.info(f"<< #2 Running for Date_Entered=True: {table} >>".format(table=table_data["table"]))
            
            #Resetting table on snowflake
            sf_sql = "delete from STG.{table} where asofdate = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
            sf_hook.run(sf_sql)
            
            #Getting the MSSQL data
            df = mssql_hook.get_pandas_df(sql="select * FROM dbo.{t} WHERE CAST(DATE_ENTERED AS DATE) = '{target_date}';".format(t=table.replace("VC_","")[:-5], target_date = table_data["asOfDate"]))
        else:
            ## << #3 Full Load >>
            if fullTableList[table]["key_column"] == None:            
                logging.info(f"<< #3 Running for full load : {table} >> ".format(table=table_data["table"]))
                
                #Resetting table on snowflake
                sf_sql = "delete from STG.{table} where asofdate = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
                sf_hook.run(sf_sql)

                #Getting the MSSQL data # Converting results to chunk
                chunks_cnt = 0                 
                for chunk in mssql_hook.get_pandas_df_by_chunks(sql="select * FROM dbo.{t};".format(t=table.replace("VC_","")[:-5]),chunksize=20000): #20k rows 
                    chunks_cnt+=1 #file counter prefix
                    dfs=[]
                    dfs.append(chunk)
                    df_chunk = pd.concat(dfs)
                    df_byte = df_chunk.replace({np.nan:'NULL'}).to_csv(sep="|", index = False, quotechar='"').encode()        
                    s3_hook.load_bytes(bytes_data=df_byte, bucket_name=s3_bucket, replace=True, key="inbound/{s3_file_path}{file}".format(s3_file_path = table_data["s3_file_path"], file = str(chunks_cnt) + '_' + table_data["file_pattern"]))
                    logging.info(f"<< #3 Uploading to s3 chunk..: {str(chunks_cnt)}_ {table} >> ".format(table = table_data["file_pattern"]))

            else:
                ## << #4 Running with KEY_COLUMN  >>
                logging.info(f"<< #4 Running for KEY_COLUMN : {table} >>".format(table=table_data["table"]))
                
                #Resetting table on snowflake
                filename_path = table_data["s3_file_path"]+table_data["file_pattern"]
                sf_sql = "delete from STG.{table} where FILENAME = '{file}';".format(table = table_data["table"], file=filename_path)
                sf_hook.run(sf_sql)

                #Will be filtering using the key columns
                #As the key column is currently ordered in increasing order, we only need to look for any greater value than previously in the DB
                key = fullTableList[table]["key_column"]
                snowflake_table = table
                snowflake_query = "select max({column}) from STG.{table}".format(column = fullTableList[table]["key_column"], table = snowflake_table)
                max_value_df = sf_hook.get_pandas_df(snowflake_query)
                max_value = max_value_df.iloc[0,0]
                #Logging the max value. A max value of 0 means that the table in snowflake is currently empty
                if max_value == None:
                    max_value = 0
                logging.info(max_value)

                df = mssql_hook.get_pandas_df(sql="select * FROM dbo.{table} where {kc} > {mv};".format(table = snowflake_table.replace("VC_","")[:-5], kc = key, mv = max_value))
        
        #Copying data to S3 bucket
        if df.empty:
            logging.info('!!! No results in datafram from MSSQL or the Table was a full load type !!!! ')
        else:
            df_byte = df.replace({np.nan:'NULL'}).to_csv(compression="gzip", sep="|", index = False, quotechar='"').encode()        
            s3_hook.load_bytes(bytes_data=df_byte, bucket_name=s3_bucket, replace=True, key="inbound/{s3_file_path}{file}".format(s3_file_path = table_data["s3_file_path"], file = table_data["file_pattern"]))

        return table_data
    
    @task()
    def copy_to_snowflake(table_details: dict):
        sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
        previous_day = table_details["asOfDate"]
        table_t = table_details["table"]
        cols = table_details["columns"]
        sql_general = "COPY INTO STG.{table_name} FROM (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{previous}'), {columns} from @ETL.INBOUND/{s3_file_path}) FILE_FORMAT = ( FORMAT_NAME = {file_format}) pattern= '.*{file_pattern}.*'"
        f_f = "STG.CM_CSV_PIPE_SH1_EON_GZ" #Will use an existing file format even though this file format was created for a different source. All file formats are interchangeable as long as they are compatible
        sql_updated = sql_general.format(table_name=table_t, columns=cols, file_format=f_f,s3_file_path=table_details["s3_file_path"], previous = previous_day, file_pattern = table_details["file_pattern"])
        sf_hook.run(sql_updated)

    @task(outlets=[ds_Staging_VC])
    def write_to_dataset():
        f = open(staging_vc_dataset, "a")
        f.write("vc data load complete")
        f.close()   
    
    for table in fullTableList:
        #with TaskGroup(group_id="tg") as tg:  #tg_CM
        tg_id="tg_{}".format(table)
        with TaskGroup(group_id=tg_id) as tg:  #tg_CM
            table_data = get_run_param(table)  
            copy_to_s3 = copy_mssql_to_s3(table_data, table)
            table_data >> copy_to_s3  >> copy_to_snowflake(copy_to_s3)
            
        start >> tg >> mid 
    mid >> write_to_dataset() >> end
ld_run = staging_VC()