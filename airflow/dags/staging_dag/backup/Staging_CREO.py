"""
### Load CREO data to Snowflake 
Scheduled to run every morning at 6AM EST
This DAG copies data for the below listed tables based on table type (increamental, full-load) to Snowflake.
"""

from datetime import datetime, timedelta
import pendulum
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import trigger_rule
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from include.functions import ms_teams_callback_functions

# ^Import list of table names
from utils.CREO_utils import fullTableListCREO as fullTableList

# ^Connections and hooks : Use DRY by defining all variables at the beginning of the script
MSSQL_CONN = "rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com"
SF_CONN = "snowflake_default"
AWS_S3_CONN = "aws_s3_conn"
S3_BUCKET = "s3_etldata_bucket_var"

mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN)
sf_hook = SnowflakeHook(snowflake_conn_id=SF_CONN)
s3_hook = S3Hook(aws_conn_id=AWS_S3_CONN)
s3_bucket = Variable.get(S3_BUCKET)

AWS_S3_CONN_DEV = "aws_s3_dev_conn" #! for local testing, comment out in prod
S3_BUCKET_DEV = "s3-dev-etldata-001" #! for local testing, comment out in prod
s3_hook = S3Hook(aws_conn_id=AWS_S3_CONN_DEV) #! for local testing, comment out in prod
s3_bucket = S3_BUCKET_DEV #! for local testing, comment out in prod

# ^Default DAG args
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
    "on_failure_callback": ms_teams_callback_functions.failure_callback
}

@dag(
    "Staging_CREO",
    start_date=pendulum.datetime(2023, 2, 28, tz='US/Eastern'),
    catchup=False,
    # schedule_interval = '0 6 * * *', #  6AM EST
    schedule_interval = None, #! Used for local testing, comment out in prod
    dagrun_timeout = timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath="include/sql/Staging_CREO/",
    default_args=default_args
)
def Staging_CREO():
    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_done") for tid in ["start", "end"]]
    
    # ^Data quality trigger
    end.trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    trigger_DQ = TriggerDagRunOperator(
        task_id='trigger_DQ_CREO',
        trigger_dag_id='DQ_CREO',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}'
    )

    # ^Get runtime parameters
    @task(multiple_outputs=True)
    def get_run_param(table_name, ds=None) -> dict:
        #// Accepts date arg so you can get data for any particular date and time
        
        # ^Define variables : Use DRY by defining all variables once at the beginning instead of in multiple string formats below
        output = sf_hook.get_first(f"SELECT ETL.COPYSELECT('STG','{table_name.upper()}',3)")
        result = output[f"ETL.COPYSELECT('STG','{table_name.upper()}',3)"] #// getting the columns $ list for each table
        table_t = table_name.replace("CREO_","")[:-5] #// truncated table name with "CREO_" prefix and "_HIST" suffix removed
        aod = datetime.strptime(ds, "%Y-%m-%d") #// datestamp input in YYYY-MM-DD format
        year, month, date = aod.strftime("%Y"), aod.strftime("%m"), aod.strftime('%Y%m%d') #// extracting year, month, date in string format from asOfDate
        pattern = f"CREO/{year}/{month}/{table_t}/" #// S3 file path
        target_file = f"{table_t}_{date}.csv.gz" #// name of CSV file that'll be stored in S3
        
        # * Returning runtime parameters to be used by copy_mssql_to_s3() and copy_to_snowflake() tasks
        return {
            "columns": result,          # get_columns
            "s3_file_path": pattern,    # The file path on the S3 location
            "asOfDate": aod,            # _asofdate
            "table": table_name,        # .*tablename_AsOfDate.csv.gz.*
            "file_pattern": target_file
        }
    
    # ^Copy data from MSSQL to S3
    @task()
    def copy_mssql_to_s3(table_data: dict, table_name: str): #// Changed `table` to `table_name` since `table` is used as variable below
        
        # ^Define variables : Use DRY by defining all variables once at the beginning instead of in multiple string formats below
        df = None
        # ** Values returned by get_run_param() func **
        table = table_data.get("table") #// value of "table" key 
        asOfDate = table_data.get("asOfDate") #// value of "asOfDate" key 
        s3_file_path = table_data.get("s3_file_path") #// value of "s3_table_path" key 
        file_pattern = table_data.get("file_pattern") #// value of "file_pattern" key 
        table_t = table.replace("CREO_","")[:-5] #// truncated table name with "CREO_" prefix and "_HIST" suffix removed

        # ** Values from "fullTableList" dictionary stored in Full_Table_Lists.py **
        table_list_name = fullTableList.get(table_name)
        if table_list_name: #// Use "if" statement to avoid Key error
            sql_file = table_list_name.get("sql")
            has_date_entered = table_list_name.get("has_date_entered")
            key_column = table_list_name.get("key_column", False) 
        
        # ^Execute tasks based on the following conditionals
        # ** 1. If the table has a custom SQL file **
        if sql_file != None:
            # * Resetting table in Snowflake
            sf_sql = f"""
                DELETE FROM STG.{table} 
                WHERE date_entered = '{asOfDate}';
            """
            sf_hook.run(sf_sql)

            # * Getting the SQL query and inserting asOfDate value
            sql_dir = "/usr/local/airflow/include/sql/Staging_CREO"
            with open(f"{sql_dir}/{sql_file}", "r") as file: #// use with to automatically close the file when done executing
                mssql_query = file.read()
                dated_mssql_query = mssql_query.format(asOfDate)
                print(dated_mssql_query)

            # * Getting the MSSQL data as pandas df
            df = mssql_hook.get_pandas_df(sql=dated_mssql_query)
            df.head(10)
        
        # ** 2. If the table does not have a custom SQL file and table has DATE_ENTERED **
        elif sql_file == None and has_date_entered:
            # * Resetting table in Snowflake
            sf_sql = f"""
                DELETE FROM STG.{table} 
                WHERE date_entered = '{asOfDate}';
            """
            sf_hook.run(sf_sql)

            # * Getting the MSSQL data 
            df = mssql_hook.get_pandas_df(sql=f"""
                SELECT * FROM dbo.{table_t} 
                WHERE CAST(DATE_ENTERED AS DATE) = '{asOfDate}';
            """)
        
        # ** 3. If the table does not have a custom SQL file and it does not have DATE_ENTERED **
        else:
            # ** 3.1. If the table does NOT have a key column **
            if key_column == None:
                # * Resetting table in Snowflake
                sf_sql = f"""
                    TRUNCATE TABLE STG.{table};
                """
                sf_hook.run(sf_sql)
                
                # * Getting the MSSQL data and converting results to chunk
                dfs = [chunk for chunk in mssql_hook.get_pandas_df_by_chunks(sql=f"SELECT * FROM dbo.{table_t};", chunksize=1000)] #10K rows 
                df = pd.concat(dfs)
            
            # ** 3.2. If the table has a key column **
            else:
                # * Resetting table in Snowflake
                filename_path = s3_file_path + file_pattern
                sf_sql = f"""
                    DELETE FROM STG.{table} 
                    WHERE FILENAME = '{filename_path}';
                """
                sf_hook.run(sf_sql)

                # * Filtering using the key columns 
                #// Key column is currently ordered in increasing order so we only need to look for any greater value than previously in the DB
                snowflake_query = f"""
                    SELECT MAX({key_column})
                    FROM STG.{table_name}
                """
                max_value_df = sf_hook.get_pandas_df(snowflake_query)
                max_value = max_value_df.iloc[0,0]
                
                # * Logging the max value 
                if max_value == None:
                    max_value = 0 #// A max value of 0 means that the table in snowflake is currently empty
                print(max_value)

                df = mssql_hook.get_pandas_df(sql=f"""
                    SELECT * 
                    FROM dbo.{table_t} 
                    WHERE {key_column} > {max_value};
                """)
        print(df.columns)
        
        # * Converting MSSQL data to compressed CSV format (*.csv.gz)
        df_byte = df\
            .replace(
                {np.nan:'NULL'}
            ).to_csv(
                compression="gzip", sep="|", 
                index = False,
                quotechar='"'
            ).encode()
        
        # * Loading compressed CSV file into S3 bucket 
        s3_hook\
            .load_bytes(
                bytes_data=df_byte, 
                bucket_name=s3_bucket, 
                replace=True, 
                key=f"inbound/{s3_file_path}{file}"
            )
        
        # * Returning input dict from get_run_param()
        return table_data

    # ^Copy data from S3 to Snowflake
    @task()
    def copy_to_snowflake(table_data: dict):
        
        # * Getting table details from copy_mssql_to_s3(), same output as get_run_param()
        previous_day = table_data.get("asOfDate")
        table_name = table_data.get("table")
        columns = table_data.get("columns")
        s3_file_path = table_data.get("s3_file_path")
        file_pattern = table_data.get("file_pattern")

        file_format = "STG.CREO_CSV_PIPE_SH1_EON_GZ"

        sql_query = f"""
            COPY INTO STG.{table_name} 
            FROM (
                SELECT 
                    METADATA$FILENAME, 
                    CURRENT_TIMESTAMP(), 
                    to_date('{previous_day}'), 
                    {columns} 
                FROM @ETL.INBOUND/{s3_file_path}
            )
            FILE_FORMAT = ( 
                FORMAT_NAME = {file_format}
            ) 
            pattern = '.*{file_pattern}.*'
        """
        sf_hook.run(sql_query)

    # ^Execute ETL for each table in tableList
    for table in fullTableList:
        tg_id=f"tg_{table}"
        with TaskGroup(group_id=tg_id) as tg:  #tg_LD
            table_data = get_run_param(table)
            copy_to_s3 = copy_mssql_to_s3(table_data, table) # this is where you export data to S3 using BCP
            table_data >> copy_to_s3  >> copy_to_snowflake(copy_to_s3) # 
        
        start >> tg >> end >> trigger_DQ

ld_run = Staging_CREO()
