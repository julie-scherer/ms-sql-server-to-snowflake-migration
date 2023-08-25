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

# Import list of table names
fullTableList = { 
	"CREO_APPROVALREQUEST_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "APPROVAL_REQUEST_KEY"
	},
	"CREO_APPROVALREQUESTITEM_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "APPROVAL_REQUEST_ITEM_KEY"
	},
	"CREO_CAMPAIGN_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "CAMPAIGN_KEY"
	},
	"CREO_CAMPAIGNTYPE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "CAMPAIGN_TYPE_KEY"
	},
	"CREO_COMMUNICATION_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "COMMUNICATION_KEY"
	},
	"CREO_COMMUNICATIONMAILING_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "COMMUNICATION_MAILING_KEY"
	},
	"CREO_CONFIG_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "CONFIG_KEY"
	},
	"CREO_CONFIGHISTORY_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "CONFIG_HISTORY_KEY"
	},
	"CREO_CONTACT_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "CONTACT_KEY"
	},
	"CREO_CONTACTTYPE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "CONTACT_TYPE_KEY"
	},
	"CREO_CONTAINER_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "CONTAINER_KEY"
	},
	"CREO_DATASET_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "DATASET_KEY"
	},
	"CREO_DATASETCELL_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_DATASETCOLUMN_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "DATASET_COLUMN_KEY"
	},
	"CREO_DATASETROW_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "DATASET_ROW_KEY"
	},
	"CREO_DATASETVALUE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "DATASET_VALUE_KEY"
	},
	"CREO_DATASOURCE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "DATASOURCE_KEY"
	},
	"CREO_DEADMESSAGES_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": None
	},
	"CREO_DEADMESSAGES2_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": None
	},
	"CREO_DELIVERYSTATUS_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "DELIVERY_STATUS_KEY"
	},
	"CREO_EMOJI_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "EMOJI_KEY"
	},
	"CREO_FOLDER_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "FOLDER_KEY"
	},
	"CREO_FOLDERCONTACT_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_FOLDERMESSAGE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_GLOBAL_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_LOG_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "LOG_KEY"
	},
	"CREO_MESSAGE_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "MESSAGE_KEY"
	},
	"CREO_MESSAGECONTACT_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "MESSAGE_CONTACT_KEY"
	},
	"CREO_MESSAGECONTACTTYPE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "MESSAGE_CONTACT_TYPE_KEY"
	},
	"CREO_MESSAGECONTACTV2_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_MESSAGEDELIVERYSTATUS_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "MESSAGE_DELIVERY_STATUS_KEY"
	},
	"CREO_MESSAGEPART_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "MESSAGE_PART_KEY"
	},
	"CREO_MESSAGEPARTV2_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_MESSAGESTATUSQUEUE_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "MESSAGE_STATUS_QUEUE_KEY"
	},
	"CREO_MESSAGETYPE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "MESSAGE_TYPE_KEY"
	},
	"CREO_PACKAGE_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "PACKAGE_KEY"
	},
	"CREO_PACKAGETEMPLATE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_PARAMETER_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "PARAMETER_KEY"
	},
	"CREO_RULE_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "RULE_KEY"
	},
	"CREO_TEMPLATE_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "TEMPLATE_KEY"
	},
	"CREO_TEMPLATERULE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": None
	},
	"CREO_TEMPLATETYPE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "TEMPLATE_TYPE_KEY"
	},
	"CREO_TEMPMESSAGE_HIST": {
		"sql": None,
		"table_filtered_by": False,
		"key_column": "TEMP_MESSAGE_KEY"
	},
	"CREO_USER_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": "USER_KEY"
	},
	"CREO_WEBHOOK_HIST": {
		"sql": None,
		"table_filtered_by": True,
		"key_column": None
	}
}


# Connections and hooks
MSSQL_CONN = "awsmssql_creosql_creo_conn" 
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

database_name = 'CREO'
file_format = 'CSV_PIPE_SH0_EON_GZ'
sql_dir = f"/usr/local/airflow/include/sql/Staging_{database_name}"

## Default DAG args
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    'execution_timeout': timedelta(hours=3),
    "on_failure_callback": ms_teams_callback_functions.failure_callback
}

@dag(
    f"Staging_{database_name}",
    start_date=pendulum.datetime(2023, 2, 28, tz='US/Eastern'),
    catchup=False,
    schedule_interval = '0 6 * * *', #  6AM EST
    # schedule_interval = '@once', #! used for local testing
    dagrun_timeout = timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath=f"include/sql/Staging_{database_name}/",
    default_args=default_args
)
def Staging_CREO():
    start, end = [EmptyOperator(task_id=tid, trigger_rule="all_done") for tid in ["start", "end"]]
    
    ## Data quality trigger
    end.trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    trigger_DQ = TriggerDagRunOperator(
        task_id=f"trigger_DQ_{database_name}",
        trigger_dag_id=f"DQ_{database_name}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}'
    )

    ## Get runtime parameters
    @task(multiple_outputs=True)
    def get_run_param(table_name, ds=None) -> dict: #// Accepts date arg so you can get data for any particular date and time
        
        ## Define variables : Use DRY by defining all variables once at the beginning instead of in multiple string formats below
        output = sf_hook.get_first(f"SELECT ETL.COPYSELECT('STG','{table_name.upper()}',3)")
        columns = output[f"ETL.COPYSELECT('STG','{table_name.upper()}',3)"] #// getting the columns $ list for each table
        table_t = table_name.replace(f"{database_name}_","")[:-5] #// truncated table name with "{database_name}_" prefix and "_HIST" suffix removed
        aod = datetime.strptime(ds, "%Y-%m-%d") #// datestamp input in YYYY-MM-DD format
        year, month, date = aod.strftime("%Y"), aod.strftime("%m"), aod.strftime('%Y%m%d') #// extracting year, month, date in string format from asOfDate
        s3_file_path = f"{database_name}/{year}/{month}/{table_t}/" #// S3 file path
        s3_csv_file = f"{table_t}_{date}.csv.gz" #// name of CSV file that'll be stored in S3
        
        # * Returning runtime parameters to be used by copy_mssql_to_s3() and copy_to_snowflake() tasks
        return {
            "table": table_name,
            "columns": columns,
            "asOfDate": aod,
            "s3_file_path": s3_file_path,
            "s3_csv_file": s3_csv_file
        }
    
    ## Copy data from MSSQL to S3
    @task()
    def copy_mssql_to_s3(runtime_params: dict, table_name: str): #// Changed `table` to `table_name` since `table` is used as variable below
        
        ## Define variables : Use DRY by defining all variables once at the beginning instead of in multiple string formats below
        df = None
        # ** Values returned by get_run_param() func **
        table = runtime_params.get("table") #// value of "table" key 
        asOfDate = runtime_params.get("asOfDate") #// value of "asOfDate" key 
        s3_file_path = runtime_params.get("s3_file_path") #// value of "s3_table_path" key 
        s3_csv_file = runtime_params.get("s3_csv_file") #// value of "s3_csv_file" key 
        table_t = table.replace(f"{database_name}_","")[:-5] #// truncated table name with "CREO_" prefix and "_HIST" suffix removed

        # ** Values from "fullTableList" dictionary stored in Full_Table_Lists.py **
        table_list_name = fullTableList.get(table_name)
        # table_list_name = get_utils_query(database_name, table_name, 'table_name')
        if table_list_name: #// Use "if" statement to avoid Key error
            sql_file = table_list_name.get("sql")
            table_filtered_by = table_list_name.get("table_filtered_by")
            key_column = table_list_name.get("key_column", False) 
            # sql_file = get_utils_query(database_name, table_name, 'sql')
            # table_filtered_by = get_utils_query(database_name, table_name, 'table_filtered_by')
            # key_column = get_utils_query(database_name, table_name, 'key_column')
        
        ## Execute tasks based on the following conditionals
        # ** 1. If the table has a custom SQL file **
        if sql_file != None:
            # * Resetting table in Snowflake
            sf_sql = f"""
                DELETE FROM STG.{table} 
                WHERE date_entered = '{asOfDate}';
            """
            sf_hook.run(sf_sql)

            # * Getting the SQL query and inserting asOfDate value
            with open(f"{sql_dir}/{sql_file}", "r") as file: #// use with to automatically close the file when done executing
                mssql_query = file.read()
                dated_mssql_query = mssql_query.format(asOfDate)
                print(dated_mssql_query)

            # * Getting the MSSQL data as pandas df
            df = mssql_hook.get_pandas_df(sql=dated_mssql_query)
            df.head(10)
        
        # ** 2. If the table does not have a custom SQL file and table has DATE_ENTERED **
        elif sql_file == None and table_filtered_by:
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
                filename_path = s3_file_path + s3_csv_file
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
            .replace({np.nan:'NULL'})\
            .to_csv(
                compression="gzip", sep="|", 
                index = False,
                quotechar='"'
            ).encode()
        
        # * Loading compressed CSV file into S3 bucket 
        s3_hook.load_bytes(
                bytes_data=df_byte, 
                bucket_name=s3_bucket, 
                replace=True, 
                key=f"inbound/{s3_file_path}{file}"
            )
        
        # * Returning input dict from get_run_param()
        return runtime_params

    ## Copy data from S3 to Snowflake
    @task()
    def copy_to_snowflake(runtime_params: dict):
        
        # * Getting table details from copy_mssql_to_s3(), same output as get_run_param()
        table_name = runtime_params.get("table")
        columns = runtime_params.get("columns")
        previous_day = runtime_params.get("asOfDate")
        s3_file_path = runtime_params.get("s3_file_path")
        s3_csv_file = runtime_params.get("s3_csv_file")

        file_format = f"STG.{database_name}_{file_format}"

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
            pattern = '.*{s3_csv_file}.*'
        """
        sf_hook.run(sql_query)

    ## Execute ETL for each table in tableList
    for table in fullTableList:
        tg_id=f"tg_{table}"
        with TaskGroup(group_id=tg_id) as tg:  #tg_LD
            runtime_params = get_run_param(table)
            copy_to_s3 = copy_mssql_to_s3(runtime_params, table)
            runtime_params >> copy_to_s3  >> copy_to_snowflake(copy_to_s3)
        
        start >> tg >> end >> trigger_DQ

staging_creo = Staging_CREO()