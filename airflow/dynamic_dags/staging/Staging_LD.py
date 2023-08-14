"""
### Load Lend Direct data to Snowflake 
This DAG copies data for the below listed tables based on table type (increamental, full-load) to Snowflake.
"""

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import logging
from airflow import DAG, macros, Dataset
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import trigger_rule
from airflow.models import Variable
from include.functions import ms_teams_callback_functions
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum

db_name = "awsmssql_canldsql_winchkcanld_conn"
snowflake = "snowflake_default"

# Data driven scheduling datasets 
staging_ld_dataset = "include/datasets/ds_staging_ld.txt"
ds_Staging_LD = Dataset(staging_ld_dataset)

fullTableList = {
    ## >> #1 Custom SQL Loads 
    'LD_InsuranceEnrollment_HIST':{"sql":'COPY_LD_INSURANCEENROLLMENT.sql',"has_date_entered":True}
    ,'LD_LoanFunding_HIST':{"sql":'COPY_LD_LOANFUNDING.sql',"has_date_entered":True}
    ,'LD_PresentmentRequest_HIST':{"sql":'COPY_LD_PRESENTMENTREQUEST.sql', "has_date_entered":True}
    ,'LD_OpenEndLoanStatement_HIST':{"sql":'COPY_LD_OPENENDLOANSTATEMENT.sql', "has_date_entered":True}
    ,'LD_OfferAccepted_HIST':{"sql":'COPY_LD_OFFERACCEPTED.sql', "has_date_entered":True}
    ,'LD_AvailableOffer_HIST':{"sql":'COPY_LD_AVAILABLEOFFER.sql', "has_date_entered":True}
    ## << #1 Custom SQL Loads 
    
    ## >> #2 Incremental load using Date_Entered    
    ,'LD_TransDetail_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_OpenEndInterest_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_MPayInterest_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_Presentment_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CustomerIncome_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_RBCEFundBatch_HIST':{"sql":None, "has_date_entered":True} 
    ,'LD_PromiseToPay_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CreditCardResultCode_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanProductConfig_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanProduct_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanPayment_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanApplication_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_SPayInterest_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_Customer_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CustomerAddress_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CreditCardAttempts_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CreditCardTrans_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_ACH_History_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_SERVICETRANS_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_SERVICEDETAIL_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CASHEDCHECK_HIST':{"sql":None, "has_date_entered":True}
    
    ## << #2 Incremental load using Date_Entered    

    ## >> #3 Full Loads 
    ,'LD_BalSheet2_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_EndOfDayRpt_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Company_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Loan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_LoanIncome_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_PromiseToPayDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_OpenEndLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_RISREPT_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_ACH_Sent_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_MPayLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Transcode_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_CapsSkipReason_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_LoanProductFinancialGroup_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    ,'LD_LoanFundingStatus_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_PresentmentType_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_District_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Region_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Currency_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Markets_HIST':{"sql":None, "has_date_entered":False, "key_column":None}        
    ,'LD_LoanPaymentMPay_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_MPayAmort_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_RBCEFundBatchDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_PresentmentRequestACHHistoryXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    ,'LD_ACHBank_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_CreditCardVendor_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_OpenEndLoanStatementBalance_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_TellerID_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_SecurityGroupHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    
    ## << #3 Full Loads    

    ## >> #4 Incremental load using key  
    ,'LD_BalSheet_TransDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_KEY"}    
    ,'LD_LoanPaymentOpenEnd_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_OPEN_END_KEY"}    
    ,'LD_TRANSDETAILACCT_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"}
    ,'LD_EndOfDayInventoryDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_KEY"}
    
    ## << #4 Incremental load using key      
    
    ## >> #5 Decommissioned
    # ,'LD_CapsCCTXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    # ,'LD_CapsRun_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    # ,'LD_SPayLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    # ,'LD_PresentmentCreditCardTransXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    # ,'LD_LoanPaymentSPay_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    # ,'LD_CapsHold_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
}


@dag(
    "Staging_LD",
    start_date=pendulum.datetime(2023, 2, 16, tz='US/Eastern'),
    catchup=False,
    schedule_interval = '0 6 * * *', # DAG Time Zone set to EST 
    dagrun_timeout = timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath="include/sql/Staging_LD/",
    tags=["Author: Shunjian Wang"],
    default_args={
        "retries": 2,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=3),
        "on_failure_callback": ms_teams_callback_functions.failure_callback
    })

def staging_LD():

    start, mid, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "mid", "end"]]

    @task(multiple_outputs=True)
    def get_run_param(table_name,ds=None,ti=None) -> dict:

        # Getting the columns $ list for each table
        sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
        output = sf_hook.get_first("SELECT ETL.COPYSELECT('STG','{}',3)".format(table_name.upper()))
        result = output["ETL.COPYSELECT('STG','{}',3)".format(table_name.upper())]
        
        # No need to manually subtract 1 day on top of that. For more info see https://tinyurl.com/yckukrnd
        # aod = datetime.strptime(ds, "%Y-%m-%d") - dateutil.relativedelta.relativedelta(days=1) 

        aod = datetime.strptime(ds, "%Y-%m-%d") 
        pattern = "LD/{year}/{month}/{day}/{table}/".format(year=aod.strftime("%Y"), month=aod.strftime("%m"), day=aod.strftime("%d"), table=table_name.replace("LD_","")[:-5])
        target_file = "{table_t}_{date}.csv.gz".format(table_t = table_name.replace("LD_","")[:-5], date=aod.strftime('%Y%m%d'))
        return {
                "columns": result,          # get_columns
                "s3_file_path": pattern,    # The file path on the S3 location
                "asOfDate": aod,            # Asofdate
                "table": table_name,        # .*tablename_AsOfDate.csv.gz.*
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
        df = pd.DataFrame() 
        s3_bucket= Variable.get("s3_etldata_bucket_var")
        
        ## << #1 Custom SQL >>
        if fullTableList[table]["sql"] != None:
            logging.info(f"<< #1 Running for Custom SQL: {table} >>".format(table=table_data["table"]))
            
            #Resetting table on snowflake
            sf_sql = "delete from STG.{table} where cast(date_entered as date) = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
            sf_hook.run(sf_sql)

            #Getting the sql query
            sql_get_data = fullTableList[table]["sql"]
            dir = "/usr/local/airflow/include/sql/Staging_LD"
            file = open("{directory}/{filename}".format(directory = dir, filename = sql_get_data), "r")
            mssql_query = file.read()
            logging.info(mssql_query.format(asOfDate=table_data["asOfDate"]))
            
            #Getting the MSSQL data  
            df = mssql_hook.get_pandas_df(sql=mssql_query.format(asOfDate=table_data["asOfDate"]))         
            
        ## << #2 Table has DATE_ENTERED and no Custom SQL >>
        elif fullTableList[table]["sql"] == None and fullTableList[table]["has_date_entered"]:
            logging.info(f"<< #2 Running for Date_Entered=True: {table} >>".format(table=table_data["table"]))
            
            #Resetting table on snowflake
            sf_sql = "delete from STG.{table} where asofdate = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
            sf_hook.run(sf_sql)
            
            #Getting the MSSQL data
            df = mssql_hook.get_pandas_df(sql="select * FROM dbo.{t} WHERE CAST(DATE_ENTERED AS DATE) = '{target_date}';".format(t=table.replace("LD_","")[:-5], target_date = table_data["asOfDate"]))
        else:
            ## << #3 Full Load >>
            if fullTableList[table]["key_column"] == None:
                logging.info(f"<< #3 Running for full load : {table} >> ".format(table=table_data["table"]))
                
                #Resetting table on snowflake
                sf_sql = "delete from STG.{table} where asofdate = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
                sf_hook.run(sf_sql)

                #Getting the MSSQL data # Converting results to chunk
                chunks_cnt = 0                 
                for chunk in mssql_hook.get_pandas_df_by_chunks(sql="select * FROM dbo.{t};".format(t=table.replace("LD_","")[:-5]),chunksize=20000): #20k rows 
                    chunks_cnt+=1 #file counter prefix
                    dfs=[]
                    dfs.append(chunk)
                    df_chunk = pd.concat(dfs)
                    df_byte = df_chunk.replace({np.nan:'NULL'}).to_csv(sep="|", index = False, quotechar='"').encode()        
                    s3_hook.load_bytes(bytes_data=df_byte, bucket_name=s3_bucket, replace=True, key="inbound/{s3_file_path}{file}".format(s3_file_path = table_data["s3_file_path"], file = str(chunks_cnt) + '_' + table_data["file_pattern"]))
                    logging.info(f"<< #3 Uploading to s3 chunk..: {str(chunks_cnt)}_ {table} >>".format(table = table_data["file_pattern"]))
            
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
                logging.info(f"MAX_VALUE: {max_value}")

                df = mssql_hook.get_pandas_df(sql="select * FROM dbo.{table} where {kc} > {mv};".format(table = snowflake_table.replace("LD_","")[:-5], kc = key, mv = max_value))

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
        sql_updated = sql_general.format(table_name=table_t, columns=cols, file_format="STG.LD_CSV_PIPE_SH1_EON_GZ",s3_file_path=table_details["s3_file_path"], previous = previous_day, file_pattern = table_details["file_pattern"])
        sf_hook.run(sql_updated)

    @task(outlets=[ds_Staging_LD])
    def write_to_dataset():
        f = open(staging_ld_dataset, "a")
        f.write("ld data load complete")
        f.close()    

    for table in fullTableList:
        #with TaskGroup(group_id="tg") as tg:  #tg_LD
        tg_id="tg_{}".format(table)
        with TaskGroup(group_id=tg_id) as tg:  #tg_LD
            table_data = get_run_param(table)  
            copy_to_s3 = copy_mssql_to_s3(table_data, table)
            table_data >> copy_to_s3 >> copy_to_snowflake(copy_to_s3)
            
        start >> tg >> mid
    mid >> write_to_dataset() >> end 
ld_run = staging_LD()
