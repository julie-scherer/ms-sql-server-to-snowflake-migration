"""
### LD DQ checks 
Triggered by.: STAGING_VC DAG.
Description..: Data Quality checks for nightly loads.
Working......: Checks are made against Source database WinchCANLD
               DAG dynamically creates checks_CM.yml for all the tables in the dictionary. And calls SODA passing checks_VC.yml. 
               Following checks are run against table dictionary (checks.yml):
                    #1 Missing(asofdate) = 0 Checks for nulls and blanks 
                    #2 Missing(filename) = 0 Checks for nulls and blanks
                    #3 duplicate_keys    = 0 Checks for a given key if tables has duplicate records ** DECOMMISSIONED **
                    #4 record_count_differential < 10 Checks for record cound difference should not be greater than 10
Table dict...: for eg: 'VC_ACH_Recv_HIST':{"Key_column":'ACH_RECV_KEY', "date_field":False}'
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

db_name = "mssql_ictvcsql_winchkictvc_conn"
snowflake = "snowflake_default"

# Data driven scheduling datasets 
staging_vc_dataset = "include/datasets/ds_staging_vc.txt"
ds_Staging_VC = Dataset(staging_vc_dataset)
dataset_name = "include/datasets/ds_dq_vc.txt"
ds_VC_DQ = Dataset(dataset_name)

fullTableList = {
    #These tables all have the date_entered field
    'VC_ACH_History_HIST':{"has_date_entered":True, "key_column":"ACH_History_Key"},
    'VC_AutoReportEditHistory_HIST':{"has_date_entered":True, "key_column":"AUTO_REPORT_EDIT_HISTORY_KEY"},
    'VC_BalSheet2_HIST':{"has_date_entered":True, "key_column":"BALSHEET_KEY"},
    'VC_BankruptcyTrustee_HIST':{"has_date_entered":True, "key_column":"BANKRUPTCY_TRUSTEE_KEY"},
    'VC_CashedCheck_HIST':{"has_date_entered":True, "key_column":"CASHED_CHECK_KEY"},
    'VC_CCardResponses_HIST':{"has_date_entered":True, "key_column":"CCARD_RESPONSE_KEY"},
    'VC_CollBonusPTP_HIST':{"has_date_entered":True, "key_column":"PTP_DETAIL_KEY"},
    'VC_CollectionAction_HIST':{"has_date_entered":True, "key_column":"COLLECTION_ACTION_KEY"},
    'VC_CollectionMovement_HIST':{"has_date_entered":True, "key_column":"COLLECTION_MOVEMENT_KEY"},
    'VC_CollectionNote_HIST':{"has_date_entered":True, "key_column":"COLLECTION_NOTE_KEY"},
    'VC_CollectionStream_HIST':{"has_date_entered":True, "key_column":"COLLECTION_STREAM_KEY"},
    'VC_CreditCardAttempts_HIST':{"has_date_entered":True, "key_column":"CREDIT_CARD_TRANS_KEY"},
    'VC_CreditCardBlock_HIST':{"has_date_entered":True, "key_column":"CREDIT_CARD_BLOCK_KEY"},
    'VC_CreditCards_HIST':{"has_date_entered":True, "key_column":"CreditCard_Key"},
    'VC_CreditCardsEdit_HIST':{"has_date_entered":True, "key_column":"CREDITCARDSEDIT_KEY"},
    'VC_CreditCardTrans_HIST':{"has_date_entered":True, "key_column":"CREDIT_CARD_TRANS_KEY"},
    'VC_CreditReportingBaseSegment_HIST':{"has_date_entered":True, "key_column":"CREDIT_REPORTING_BASE_SEGMENT_KEY"},
    'VC_CreditReportingBaseSegmentHistory_HIST':{"has_date_entered":True, "key_column":"CREDIT_REPORTING_BASE_SEGMENT_KEY"},
    'VC_CreditReportingLoanActivity_HIST':{"has_date_entered":True, "key_column":"CREDIT_REPORTING_LOAN_ACTIVITY_KEY"},
    'VC_CreditReportingLoanDisputeNote_HIST':{"has_date_entered":True, "key_column":"CREDIT_REPORTING_LOAN_DISPUTE_NOTE_KEY"},
    'VC_CreditReportingProcessingQueue_HIST':{"has_date_entered":True, "key_column":"CREDIT_REPORTING_PROCESSING_QUEUE_KEY"},
    'VC_CreditReportingProcessingQueueHistory_HIST':{"has_date_entered":True, "key_column":"CREDIT_REPORTING_PROCESSING_QUEUE_KEY"},
    'VC_CreditVendorData_HIST':{"has_date_entered":True, "key_column":"CREDIT_VENDOR_DATA_KEY"},
    'VC_CustomerAddress_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_ADDRESS_KEY"},
    'VC_CustomerAddressEdit_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_ADDRESS_EDIT_KEY"},
    'VC_CustomerEdit_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_EDIT_KEY"},
    'VC_CustomerEmployerEdit_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_EMPLOYER_EDIT_KEY"},
    'VC_CustomerNote_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_NOTE_KEY"},
    'VC_CustomerPhoneNumber_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_PHONE_NUMBER_KEY"},
    'VC_CustomerPhoneNumberEdit_HIST':{"has_date_entered":True, "key_column":"CUSTOMER_PHONE_NUMBER_EDIT_KEY"}, 
    'VC_DocuwareDocument_HIST':{"has_date_entered":True, "key_column":"DOCUWARE_DOCUMENT_KEY"},
    'VC_DocuwareID_HIST':{"has_date_entered":True, "key_column":"DOCUWARE_ID_KEY"},
    'VC_DocuwareVisitorEmailAttachmentXref_HIST':{"has_date_entered":True, "key_column":"DOCUWARE_VISITOR_EMAIL_ATTACHMENT_XREF_KEY"},
    'VC_DrawerZ_HIST':{"has_date_entered":True, "key_column":"DRAWERZ_KEY"},
    'VC_EndOfDayRpt_HIST':{"has_date_entered":True, "key_column":"EODR_KEY"},
    'VC_ExcludeFromCapsHistory_HIST':{"has_date_entered":True, "key_column":"EXCLUDE_FROM_CAPS_HISTORY_KEY"},
    'VC_IssuerEdit_HIST':{"has_date_entered":True, "key_column":"ISSUER_EDIT_KEY"},
    'VC_LoanChkAcctChange_HIST':{"has_date_entered":True, "key_column":"CHK_ACCT_CHANGE_KEY"},
    'VC_LoanDepositStatusHistory_HIST':{"has_date_entered":True, "key_column":"LOAN_DEPOSIT_STATUS_HISTORY_KEY"},
    'VC_LoanDueDateChange_HIST':{"has_date_entered":True, "key_column":"DUEDATE_CHANGE_KEY"},
    'VC_LoanNote_HIST':{"has_date_entered":True, "key_column":"LOAN_NOTE_KEY"},
    'VC_LoanPayment_HIST':{"has_date_entered":True, "key_column":"LOAN_PAYMENT_KEY"},
    'VC_LoanPaymentCheckPaymentTypeXref_HIST':{"has_date_entered":True, "key_column":"LOAN_PAYMENT_CHECK_PAYMENT_TYPE_XREF_KEY"},
    'VC_LoanPaymentRefund_HIST':{"has_date_entered":True, "key_column":"LOAN_PAYMENT_REFUND_KEY"},
    'VC_LoanPaymentStaged_HIST':{"has_date_entered":True, "key_column":"LOAN_PAYMENT_STAGED_KEY"},
    'VC_LoanPayoffDate_HIST':{"has_date_entered":True, "key_column":"LOAN_PAYOFF_DATE_KEY"},
    'VC_LoanStatusChange_HIST':{"has_date_entered":True, "key_column":"LOAN_STATUS_CHANGE_KEY"},
    'VC_LocationUS_ZipcodesXRef_HIST':{"has_date_entered":True, "key_column":"ZIPCODE"},
    'VC_MPayInterest_HIST':{"has_date_entered":True, "key_column":"MPAY_INTEREST_KEY"},
    'VC_MPayLoanInSyncAdj_HIST':{"has_date_entered":True, "key_column":"MPAY_LOAN_IN_SYNC_ADJ_KEY"},
    'VC_MPayRecalcLoanPaymentAdj_HIST':{"has_date_entered":True, "key_column":"MPAY_RECALC_LOAN_PAYMENT_ADJ_KEY"},
    'VC_OverShort_HIST':{"has_date_entered":True, "key_column":"OS_KEY"},
    'VC_PaymentsPastDue_HIST':{"has_date_entered":True, "key_column":"PAYMENTS_PAST_DUE_KEY"},
    'VC_Presentment_HIST':{"has_date_entered":True, "key_column":"PRESENTMENT_KEY"},
    'VC_PresentmentRequest_HIST':{"has_date_entered":True, "key_column":"PRESENTMENT_REQUEST_KEY"},
    'VC_PresentmentRequestNotSent_HIST':{"has_date_entered":True, "key_column":"PRESENTMENT_REQUEST_NOT_SENT_KEY"},
    'VC_PromiseToPay_HIST':{"has_date_entered":True, "key_column":"PROMISE_TO_PAY_KEY"},
    'VC_PromiseToPayCommunication_HIST':{"has_date_entered":True, "key_column":"PROMISE_TO_PAY_COMMUNICATION_KEY"},
    'VC_PromiseToPayDetailEdit_HIST':{"has_date_entered":True, "key_column":"PROMISE_TO_PAY_DETAIL_EDIT_KEY"},
    'VC_Receipt_HIST':{"has_date_entered":True, "key_column":"RECEIPT_KEY"},
    'VC_TellerIDEdit_HIST':{"has_date_entered":True, "key_column":"TELLER_ID_EDIT_KEY"},
    'VC_TellerPwdHistory_HIST':{"has_date_entered":True, "key_column":"TELLER_PWD_HISTORY_KEY"},
    'VC_TellerSecurity_HIST':{"has_date_entered":True, "key_column":"TELLER_SECURITY_KEY"},
    'VC_TransDetail_HIST':{"has_date_entered":True, "key_column":"TRANS_DETAIL_KEY"},
    'VC_TransPOS_HIST':{"has_date_entered":True, "key_column":"TRANS_POS_KEY"},
    'VC_VaultCount_HIST':{"has_date_entered":True, "key_column":"VAULT_COUNT_KEY"},
    'VC_VaultRecalcAdj_HIST':{"has_date_entered":True, "key_column":"VAULT_RECALC_ADJ_KEY"},
    'VC_VisitorAuthenticationCode_HIST':{"has_date_entered":True, "key_column":"VISITOR_AUTHENTICATION_CODE_KEY"},
    'VC_VisitorAuthenticationCodeAttempt_HIST':{"has_date_entered":True, "key_column":"VISITOR_AUTHENTICATION_CODE_ATTEMPT_KEY"},
    'VC_VisitorDevice_HIST':{"has_date_entered":True, "key_column":"VISITOR_DEVICE_KEY"},
    'VC_VisitorDocument_HIST':{"has_date_entered":True, "key_column":"VISITOR_DOCUMENT_KEY"},
    'VC_VisitorEdit_HIST':{"has_date_entered":True, "key_column":"VISITOR_EDIT_KEY"},
    'VC_VisitorEmail_HIST':{"has_date_entered":True, "key_column":"VISITOR_EMAIL_KEY"},
    'VC_VisitorPasswordHistory_HIST':{"has_date_entered":True, "key_column":"VISITOR_PASSWORD_HISTORY_KEY"},
    'VC_WebPixelVendorData_HIST':{"has_date_entered":True, "key_column":"WEB_PIXEL_VENDOR_DATA_KEY"},

    #These tables do not have the date_entered field and must be loaded using another strategy
    'VC_ACH_Recv_HIST':{"has_date_entered":False, "key_column":"ACH_RECV_KEY"},
    'VC_ACH_Sent_HIST':{"has_date_entered":False, "key_column":"ACH_SENT_KEY"},
    'VC_ACHSentParent_HIST':{"has_date_entered":False, "key_column":"ACH_SENT_PARENT_KEY"},
    'VC_BalSheet_TransDetail_HIST':{"has_date_entered":False, "key_column":"BALSHEET_KEY"},
    'VC_BalSheetColumns2_HIST':{"has_date_entered":False, "key_column":"BSC_KEY"},
    'VC_BankAccount_HIST':{"has_date_entered":False, "key_column":"Bank_account_KEY"},
    'VC_BatchExecution_HIST':{"has_date_entered":False, "key_column":"Batch_EXECUTION_KEY"},
    'VC_CapsRun_HIST':{"has_date_entered":False, "key_column":"CAPS_RUN_KEY"},
    'VC_CollectionAgingItem_HIST':{"has_date_entered":False, "key_column":"COLLECTION_AGING_CONFIG_DAYS_KEY"},
    'VC_CreditReportingLoanDispute_HIST':{"has_date_entered":False, "key_column":"CREDIT_REPORTING_LOAN_DISPUTE_KEY"},
    'VC_CreditReportingLoanStatusHistory_HIST':{"has_date_entered":False, "key_column":"LOAN_KEY"},
    'VC_CreditReportingRun_HIST':{"has_date_entered":False, "key_column":"CREDIT_REPORTING_RUN_KEY"},
    'VC_DepositChk_HIST':{"has_date_entered":False, "key_column":"DEPOSIT_CHK_KEY"},
    'VC_DepositChkDetail_HIST':{"has_date_entered":False, "key_column":"DEPOSIT_CHK_DETAIL_KEY"},
    'VC_DialerKeys_HIST':{"has_date_entered":False, "key_column":"DIALER_KEYS_KEY"},
    'VC_DocuwareLoanDoc_HIST':{"has_date_entered":False, "key_column":"DOCUWARE_LOAN_DOC_KEY"},
    'VC_DocuwareStatus_HIST':{"has_date_entered":False, "key_column":"DocuwareStatus_KEY"},
    'VC_DocuwareVisitorDocXRef_HIST':{"has_date_entered":False, "key_column":"VISITOR_DOCUMENT_KEY"},
    'VC_DrawerMaster_HIST':{"has_date_entered":False, "key_column":"Drawer_key"},
    'VC_DrawerZCash_HIST':{"has_date_entered":False, "key_column":"DRAWERZ_CASH_KEY"},
    'VC_EndOfDayInventoryDetail_HIST':{"has_date_entered":False, "key_column":"EODR_KEY"},
    'VC_EndOfDayRptDetail_HIST':{"has_date_entered":False, "key_column":"EODR_DET_KEY"},
    'VC_FormLetterBatch_HIST':{"has_date_entered":False, "key_column":"FORM_LETTER_BATCH_KEY"},
    'VC_FormLetterBatchVendorFile_HIST':{"has_date_entered":False, "key_column":"FORM_LETTER_BATCH_BUILD_VENDOR_FILE_KEY"},
    'VC_FormLetterPrinted_HIST':{"has_date_entered":False, "key_column":"FORM_LETTER_PRINTED_KEY"},
    'VC_FormLetterResult_HIST':{"has_date_entered":False, "key_column":"FORM_LETTER_RESULT_KEY"},
    'VC_GLAcct_HIST':{"has_date_entered":False, "key_column":"GL_ACCT_KEY"},   
    'VC_Issuer_HIST':{"has_date_entered":False, "key_column":"ISSUER_KEY"},
    'VC_LoanAuthorizedPaymentMethod_HIST':{"has_date_entered":False, "key_column":"LOAN_AUTHORIZED_PAYMENT_METHOD_KEY"},
    'VC_LoanFundingHistory_HIST':{"has_date_entered":False, "key_column":"LOAN_FUNDING_HISTORY_KEY"},
    'VC_LoanFundingHistoryDetail_HIST':{"has_date_entered":False, "key_column":"LOAN_FUNDING_HISTORY_DETAIL_KEY"},
    'VC_LoanPaymentDecreaseAmountOwed_HIST':{"has_date_entered":False, "key_column":"LOAN_PAYMENT_DECREASE_AMOUNT_OWED_KEY"},
    'VC_LoanPaymentDueDate_HIST':{"has_date_entered":False, "key_column":"LOAN_PAYMENT_DUE_DATE_KEY"},
    'VC_LoanPaymentMPay_HIST':{"has_date_entered":False, "key_column":"LOAN_PAYMENT_MPAY_KEY"},
    'VC_LoanPaymentSuspendInterest_HIST':{"has_date_entered":False, "key_column":"LOAN_PAYMENT_SUSPEND_INTEREST_KEY"},
    'VC_PaymentsPastDueDetail_HIST':{"has_date_entered":False, "key_column":"PAYMENTS_PAST_DUE_DETAIL_KEY"},
    'VC_PresentmentCreditCardTransXRef_HIST':{"has_date_entered":False, "key_column":"CREDIT_CARD_TRANS_KEY"},
    'VC_PresentmentRequestACHHistoryXRef_HIST':{"has_date_entered":False, "key_column":"ACH_HISTORY_KEY"},
    'VC_PresentmentRequestNotSentReason_HIST':{"has_date_entered":False, "key_column":"PRESENTMENT_REQUEST_NOT_SENT_REASON_KEY"},
    'VC_PromiseToPayDetail_HIST':{"has_date_entered":False, "key_column":"PTP_DETAIL_KEY"},
    'VC_PromiseToPayDetailTrans_HIST':{"has_date_entered":False, "key_column":"PTP_DETAIL_TRANS_KEY"},
    'VC_RISREPT_HIST':{"has_date_entered":False, "key_column":"RISREPT_KEY"},
    'VC_RisReptDoNotContact_HIST':{"has_date_entered":False, "key_column":"RIS_REPT_DO_NOT_CONTACT_KEY"},
    'VC_Store_Windows_HIST':{"has_date_entered":False, "key_column":"Store_Windows_Key"},
    'VC_TELLERID_HIST':{"has_date_entered":False, "key_column":"TELLER_ID_KEY"},
    'VC_TellerLogin_HIST':{"has_date_entered":False, "key_column":"TELLER_LOGIN_KEY"},
    'VC_TransDetailAcct_HIST':{"has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"},
    'VC_TransDetailCash_HIST':{"has_date_entered":False, "key_column":"TRANS_DETAIL_CASH_KEY"},
    'VC_TransDetailCashParsedCash_HIST':{"has_date_entered":False, "key_column":"TRANS_DETAIL_CASH_PARSED_CASH_KEY"},
    'VC_TransDetailCheck_HIST':{"has_date_entered":False, "key_column":"TRANS_DETAIL_CHECK_KEY"},
    'VC_TransDetailLoan_HIST':{"has_date_entered":False, "key_column":"TRANS_DETAIL_LOAN_KEY"},
    'VC_US_Zipcodes_HIST':{"has_date_entered":False, "key_column":"ZIPCODE"},
    'VC_VaultMgrAuthorization_HIST':{"has_date_entered":False, "key_column":"VM_AUTH_KEY"},
    'VC_VaultMgrAuthorizationDetail_HIST':{"has_date_entered":False, "key_column":"VM_AUTH_DETAIL_KEY"},
    'VC_VisitorCommunicationPreference_HIST':{"has_date_entered":False, "key_column":"VISITOR_COMMUNICATION_PREFERENCE_KEY"},
    'VC_VisitorEmailDisposition_HIST':{"has_date_entered":False, "key_column":"VISITOR_EMAIL_DISPOSITION_KEY"},
    'VC_WebCallCenterLogin_HIST':{"has_date_entered":False, "key_column":"CallCenter_Login_Key"},
    'VC_WebCallQueue_HIST':{"has_date_entered":False, "key_column":"WEB_CALL_QUEUE_KEY"},
    'VC_WebCallQueueAudit_HIST':{"has_date_entered":False, "key_column":"WEB_CALL_QUEUE_AUDIT_KEY"},
    'VC_WebCallRARRHistory_HIST':{"has_date_entered":False, "key_column":"WEB_CALL_RARR_HISTORY_KEY"},
    'VC_WebCallUserSetting_HIST':{"has_date_entered":False, "key_column":"WEB_CALL_USER_SETTING_KEY"},
    'VC_WebCallWorkItemCategoryHistory_HIST':{"has_date_entered":False, "key_column":"WEB_CALL_WORK_ITEM_CATEGORY_HISTORY_KEY"},
    'VC_WebCallWorkQueue_HIST':{"has_date_entered":False, "key_column":"WEB_CALL_WORK_QUEUE_KEY"}

    #These tables will be loaded via a full snapshot for every day with an additional view created from the table where only the most recent days worth of data is included
    #,'VC_GlobalHistory_HIST':{"has_date_entered":False, "key_column":None}
}

with DAG(
    dag_id="DQ_VC",
    catchup=False,
    start_date=pendulum.datetime(2023, 5, 5, tz='US/Eastern'),
    schedule = [ds_Staging_VC],
    #schedule_interval = None, # DAG Time Zone set to EST 
    doc_md=__doc__,
    tags=["Author: Shunjian Wang","cross-DAG dependencies", "Staging_VC"],
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
        config_file_updated = open("{directory}/config/configuration_parametrized_VC.yml".format(directory = SODA_PATH),"w")
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
        file2 = open(f"{SODA_PATH}/checks/checks_VC.yml","a")

        for table in fullTableList:
            
            key = fullTableList[table]["key_column"]
            key1 = key.split(',')[0]
            logging.info("The key for this table is {}".format(key1))
            
            table_name = table
            mssql_hook = MsSqlHook(mssql_conn_id=db_name)
            sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
            df = None
            
            snowflake_query_min = "select max({column}) from STG.{table} WHERE asOfDate < '{date}'".format(column = key1, table = table, date = asOfDate)
            min_value_df = sf_hook.get_pandas_df(snowflake_query_min)
            min_value = min_value_df.iloc[0,0]
            if min_value == None:
                min_value = 0

            snowflake_query_max = "select max({column}) from STG.{table}".format(column = key1, table = table)
            max_value_df = sf_hook.get_pandas_df(snowflake_query_max)
            max_value = max_value_df.iloc[0,0]
            if max_value == None:
                max_value = 0
            
            if fullTableList[table_name]["has_date_entered"]:
                df = mssql_hook.get_pandas_df(sql="select count(*) FROM dbo.{t} WHERE CAST(DATE_ENTERED AS DATE) = '{target_date}' AND {key} > {floor} and {key} <= {ceiling};".format(t=table_name.replace("VC_","")[:-5], target_date = asOfDate, key = key1, floor = min_value, ceiling = max_value))
            else:
                df = mssql_hook.get_pandas_df(sql="select count(*) FROM dbo.{t} WHERE {key} > {floor} and {key} <= {ceiling};".format(t=table.replace("VC_","")[:-5], key = key1, floor = min_value, ceiling = max_value))
            
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
        checks_file_name = f"{SODA_PATH}/checks/checks_VC.yml"
        checks_file = open(checks_file_name,"r")
        logging.info("Printing checks contents")
        logging.info(checks_file.read())
        checks_file.close()

    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "end"]]
    
    #Operators for creating and cleaning up the initial checks file. Just an empty file is sufficient as it will have its values/parameters dynamically written in during the set config phase
    remove_checks = BashOperator(
        task_id='remove_checks',
        bash_command='rm -f /usr/local/airflow/include/soda/checks/checks_VC.yml',
    )

    create_checks = BashOperator(
        task_id='create_checks',
        bash_command='touch /usr/local/airflow/include/soda/checks/checks_VC.yml',
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
        task_id="dq_VC",
        bash_command=f"soda scan -d ds -c {SODA_PATH}/config/configuration_parametrized_VC.yml {SODA_PATH}/checks/checks_VC.yml"
    )

    @task(outlets=[ds_VC_DQ])
    def write_to_dataset():
        f = open(dataset_name, "a")        
        f.write("VC DQ complete")
        logging.info("Updated the dataset file")
        f.close()

start >> remove_checks >> create_checks >> set_conf >> safety_check >> soda_scan >> write_to_dataset() >> end
#start >> remove_checks >> create_checks >> set_conf >> safety_check >> soda_scan >> end