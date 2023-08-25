"""
### Load Cash Money data to Snowflake 
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

db_name = "awsmssql_cansql_winchkcan_conn"
snowflake = "snowflake_default"

# Data driven scheduling datasets 
staging_cm_dataset = "include/datasets/ds_staging_cm.txt"
ds_Staging_CM = Dataset(staging_cm_dataset)

fullTableList = {
    ## >> #1 Custom SQL Loads 
    'CM_PRESENTMENTREQUEST_HIST':{"sql":'COPY_CM_PRESENTMENTREQUEST.sql', "has_date_entered":True},
    'CM_OPENENDLOANSTATEMENT_HIST':{"sql":'COPY_CM_OPENENDLOANSTATEMENT.sql', "has_date_entered":True},
    'CM_INSURANCEENROLLMENT_HIST':{"sql":'COPY_CM_INSURANCEENROLLMENT.sql', "has_date_entered":True},
    'CM_LOANFUNDING_HIST':{"sql":'COPY_CM_LOANFUNDING.sql', "has_date_entered":True},
    'CM_OFFERACCEPTED_HIST':{"sql":'COPY_CM_OFFERACCEPTED.sql', "has_date_entered":True},
    'CM_AVAILABLEOFFER_HIST':{"sql":'COPY_CM_AVAILABLEOFFER.sql', "has_date_entered":True},
    ## << #1 Custom SQL Loads 
    
    ## >> #2 Incremental load using Date_Entered
    'CM_TRANSDETAIL_HIST':{"sql":None, "has_date_entered":True},
    'CM_ACH_HISTORY_HIST':{"sql":None, "has_date_entered":True},    
    'CM_COMPANY_HIST':{"sql":None, "has_date_entered":True},
    'CM_CREDITCARDATTEMPTS_HIST':{"sql":None, "has_date_entered":True},
    'CM_CREDITCARDRESULTCODE_HIST':{"sql":None, "has_date_entered":True},
    'CM_CREDITCARDTRANS_HIST':{"sql":None, "has_date_entered":True},
    'CM_CUSTOMER_HIST':{"sql":None, "has_date_entered":True},
    'CM_CUSTOMERADDRESS_HIST':{"sql":None, "has_date_entered":True},
    'CM_CUSTOMERINCOME_HIST':{"sql":None, "has_date_entered":True},
    'CM_LOANAPPLICATION_HIST':{"sql":None, "has_date_entered":True},    
    'CM_LOANPAYMENT_HIST':{"sql":None, "has_date_entered":True},
    'CM_LOANPRODUCT_HIST':{"sql":None, "has_date_entered":True},
    'CM_LOANPRODUCTCONFIG_HIST':{"sql":None, "has_date_entered":True},
    'CM_MPAYINTEREST_HIST':{"sql":None, "has_date_entered":True},
    'CM_OPENENDINTEREST_HIST':{"sql":None, "has_date_entered":True},   
    'CM_PRESENTMENT_HIST':{"sql":None, "has_date_entered":True},    
    'CM_PROMISETOPAY_HIST':{"sql":None, "has_date_entered":True},
    'CM_RBCEFUNDBATCH_HIST':{"sql":None, "has_date_entered":True},
    'CM_SPAYINTEREST_HIST':{"sql":None, "has_date_entered":True},
    'CM_SERVICETRANS_HIST':{"sql":None, "has_date_entered":True},
    'CM_SERVICEDETAIL_HIST':{"sql":None, "has_date_entered":True},
    'CM_CASHEDCHECK_HIST':{"sql":None, "has_date_entered":True}, 
    # << #2 Incremental load using Date_Entered
    
    ## >> #3 Full Loads 
    'CM_BALSHEET2_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_ENDOFDAYRPT_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_LOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_CAPSRUN_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_CREDITCARDVENDOR_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_CURRENCY_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_DISTRICT_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_ACH_SENT_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_LOANFUNDINGSTATUS_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_LOANPRODUCTFINANCIALGROUP_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_MARKETS_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_PRESENTMENTCREDITCARDTRANSXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_PRESENTMENTTYPE_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_PROMISETOPAYDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_REGION_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_OPENENDLOANSTATEMENTBALANCE_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_TELLERID_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_SECURITYGROUPHISTORY_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    
    ## << #3 Full Loads 
    
    ## >> #4 Incremental load using key   
    'CM_BALSHEET_TRANSDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_KEY"},
    'CM_ENDOFDAYINVENTORYDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_KEY"},
    'CM_LOANINCOME_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_INCOME_KEY"},
    'CM_LOANPAYMENTMPAY_HIST':{"sql":None, "has_date_entered":False, "key_column":"loan_payment_mpay_key"},
    'CM_LOANPAYMENTOPENEND_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_OPEN_END_KEY"},
    'CM_LOANPAYMENTSPAY_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_SPAY_KEY"},
    'CM_MPAYAMORT_HIST':{"sql":None, "has_date_entered":False, "key_column":"MPAY_AMORT_KEY"},
    'CM_MPAYLOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":"MPAY_LOAN_KEY"},    
    'CM_OPENENDLOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":"OPEN_END_LOAN_KEY"},
    'CM_PRESENTMENTREQUESTACHHISTORYXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_HISTORY_KEY"},
    'CM_TRANSCODE_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_CODE_KEY"},
    'CM_RBCEFUNDBATCHDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":"RBC_EFUND_BATCH_DETAIL_KEY"},
    'CM_RISREPT_HIST':{"sql":None, "has_date_entered":False, "key_column":"RISREPT_KEY"},
    'CM_SPAYLOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":"SPAY_LOAN_KEY"},
    'CM_TRANSDETAILACCT_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"}
    # ## << #4 Incremental load using key   
    
    ## >> #5 Decommissioned
    #'CM_CAPSCCTXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":None},   # Zero recrods in Winchk
    #'CM_CAPSHOLD_HIST':{"sql":None, "has_date_entered":False, "key_column":None},      # Zero recrods in Winchk
    #'CM_CAPSSKIPREASON_HIST':{"sql":None, "has_date_entered":False, "key_column":None},# One time backfill
}

@dag(
    "Staging_CM",
    start_date=pendulum.datetime(2023, 2, 28, tz='US/Eastern'),
    catchup=False,
    schedule_interval = '0 6 * * *', #  6AM EST
    dagrun_timeout = timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath="include/sql/Staging_CM/",
    tags=["Author: Shunjian Wang"],
    default_args={
        "retries": 0,
        "owner": "Data Engineering",
        "retry_delay": timedelta(minutes=1),
        'execution_timeout': timedelta(hours=3),
        "on_failure_callback": ms_teams_callback_functions.failure_callback
    })

def staging_CM():
    start, mid, end = [EmptyOperator(task_id=tid, trigger_rule="all_success") for tid in ["start", "mid", "end"]]

    @task(multiple_outputs=True)
    def get_run_param(table_name,ds=None,ti=None) -> dict:
        sf_hook = SnowflakeHook(snowflake_conn_id=snowflake)
        output = sf_hook.get_first("SELECT ETL.COPYSELECT('STG','{table}',3)".format(table=table_name.upper()))
        result = output["ETL.COPYSELECT('STG','{table}',3)".format(table=table_name.upper())]
        aod = datetime.strptime(ds, "%Y-%m-%d") 

        pattern = "CM/{year}/{month}/{day}/{table}/".format(year=aod.strftime("%Y"), month=aod.strftime("%m"), day=aod.strftime("%d"), table=table_name.replace("CM_","")[:-5])
        target_file = "{table_t}_{date}.csv.gz".format(table_t = table_name.replace("CM_","")[:-5], date=aod.strftime('%Y%m%d'))
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
            dir = "/usr/local/airflow/include/sql/Staging_CM"
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
            df = mssql_hook.get_pandas_df(sql="select * FROM dbo.{t} WHERE CAST(DATE_ENTERED AS DATE) = '{target_date}';".format(t=table.replace("CM_","")[:-5], target_date = table_data["asOfDate"]))
        else:
            ## << #3 Full Load >>
            if fullTableList[table]["key_column"] == None:            
                logging.info(f"<< #3 Running for full load : {table} >> ".format(table=table_data["table"]))
                
                #Resetting table on snowflake
                sf_sql = "delete from STG.{table} where asofdate = to_date('{asOfDate}');".format(table = table_data["table"], asOfDate=table_data["asOfDate"])
                sf_hook.run(sf_sql)

                #Getting the MSSQL data # Converting results to chunk
                chunks_cnt = 0                 
                for chunk in mssql_hook.get_pandas_df_by_chunks(sql="select * FROM dbo.{t};".format(t=table.replace("CM_","")[:-5]),chunksize=20000): #20k rows 
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

                df = mssql_hook.get_pandas_df(sql="select * FROM dbo.{table} where {kc} > {mv};".format(table = snowflake_table.replace("CM_","")[:-5], kc = key, mv = max_value))
        
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
        f_f = "STG.CM_CSV_PIPE_SH1_EON_GZ"
        sql_updated = sql_general.format(table_name=table_t, columns=cols, file_format=f_f,s3_file_path=table_details["s3_file_path"], previous = previous_day, file_pattern = table_details["file_pattern"])
        sf_hook.run(sql_updated)

    @task(outlets=[ds_Staging_CM])
    def write_to_dataset():
        f = open(staging_cm_dataset, "a")
        f.write("cm data load complete")
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
ld_run = staging_CM()