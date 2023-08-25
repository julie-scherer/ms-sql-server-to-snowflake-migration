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
import shutil
import logging
from pathlib import Path
import subprocess
from datetime import datetime, timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions


# - - - - - - - - - SETTINGS - - - - - - - - -

author = 'Julie Scherer'
fullTableList = [
    # "APPROVALREQUEST", "APPROVALREQUESTITEM", "CAMPAIGN", "CAMPAIGNTYPE", 
    "COMMUNICATION", "COMMUNICATIONMAILING", "CONFIG", "CONFIGHISTORY", 
    # "CONTACT", "CONTACTTYPE", "CONTAINER", "DATASET", "DATASETCELL", 
    "DATASETCOLUMN", "DATASETROW", "DATASETVALUE", "DATASOURCE", "DEADMESSAGES", 
    # "DEADMESSAGES2", "DELIVERYSTATUS", "EMOJI", "FOLDER", "FOLDERCONTACT", 
    "FOLDERMESSAGE", "GLOBAL", "LOG", "MESSAGE", "MESSAGECONTACT", "MESSAGECONTACTTYPE", 
    # "MESSAGECONTACTV2", "MESSAGEDELIVERYSTATUS", "MESSAGEPART", "MESSAGEPARTV2", 
    # "MESSAGESTATUSQUEUE", "MESSAGETYPE", "PACKAGE", "PACKAGETEMPLATE", "PARAMETER", 
    # "RULE", "TEMPLATE", "TEMPLATERULE", "TEMPLATETYPE", "TEMPMESSAGE", "USER", "WEBHOOK"
]

# Constants >> Defined in upper case to distinguish from locally defined variables
DATABASE_NAME = "CREO"
FILE_FORMAT = "FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ"
SQL_DIR = f"/usr/local/airflow/include/sql/Staging_{DATABASE_NAME}"
START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')
TESTING = False


# - - - - - - CONNECTIONS & HOOKS - - - - - - -


def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_CONN = "awsmssql_creosql_creo_conn"
    MSSQL_HOOK = MsSqlHook(mssql_conn_id=MSSQL_CONN)
    return MSSQL_HOOK

def get_s3_hook():
    """
    Returns a S3 hook for interacting with S3.
    """
    S3_CONN = "aws_s3_conn"
    S3_HOOK = S3Hook(aws_conn_id=S3_CONN)
    return S3_HOOK

def get_sf_hook():
    """
    Returns a Snowflake hook for interacting with Snowflake.
    """
    SF_CONN = "snowflake_default"
    SF_HOOK = SnowflakeHook(snowflake_conn_id=SF_CONN)
    return SF_HOOK


# - - - - - - SNOWFLAKE UTILS TABLE - - - - - - -

def get_table_utils(table_name):
    utils_query = f"""
            SELECT * FROM ARES.ETL.{DATABASE_NAME}_UTILS
            WHERE table_name = '{DATABASE_NAME}_{table_name}_HIST'
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
    "Staging_CREO_BCP",
    start_date=START_DATE,
    catchup=False,
    schedule='0 6 * * *',  # 6AM EST
    dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    template_searchpath=f"include/sql/Staging_{DATABASE_NAME}/",
    default_args=default_args
)
def Staging_CREO_BCP():

    @task(multiple_outputs=True)
    def get_runtime_params(table_name, ds=None) -> dict:
        """
        Fetches runtime parameters and sets them in the context dictionary.
        
        Retrieves runtime parameters and generates file and directory paths for data export.
        
        Parameters:
            table_name (str): The name of the Snowflake table to export data to.
            ds (str, optional): The date string in the format "YYYY-MM-DD". Defaults to None.
            
        Returns:
            dict: A dictionary containing the following runtime parameters:
                - table_name (str): The table name without the database prefix and "_HIST".
                - columns (str): The columns to be copied.
                - aod (datetime): The date as a datetime object.
                - s3_bucket_name (str): The name of the S3 bucket for data storage.
                - s3_dir_path (str): The S3 bucket file path where the data will be stored.
                - file_name (str): The name of the file to be created.
                - csv_file_name (str): The name of the CSV file to be created.
                - zip_file_name (str): The name of the ZIP file to be created.
                - table_utils (dict): A dictionary containing table utilities.
        """

        query_output = get_sf_hook().get_first(f"""SELECT ETL.COPYSELECT('STG','{DATABASE_NAME}_{table_name}_HIST',3)""")
        columns = query_output[f"ETL.COPYSELECT('STG','{DATABASE_NAME}_{table_name}_HIST',3)"]
        aod = datetime.strptime(ds, "%Y-%m-%d")
        s3_bucket_name = Variable.get("s3_etldata_bucket_var")
        s3_dir_path = aod.strftime(f"{DATABASE_NAME}/%Y/%m/%d/{table_name}")

        local_output_dir = aod.strftime(f"include/bcp/Staging_{DATABASE_NAME}/%Y/%m/%d/{table_name}")
        os.makedirs(local_output_dir, exist_ok=True) # Create the local directory
        Path(local_output_dir).mkdir(parents=True, exist_ok=True) 
        logging.info(f"Local output directory created: {local_output_dir}")

        csv_file_name = aod.strftime(f"{table_name}_%Y%m%d.csv")
        zip_file_name = csv_file_name + '.gz'

        csv_out_path = os.path.join(local_output_dir, csv_file_name)
        Path(csv_out_path).touch(exist_ok=True)
        logging.info(f"Local CSV file created: {csv_out_path}")

        logs_dir = datetime.now().strftime(f"include/logs/Staging_{DATABASE_NAME}/%Y/%m/%d/{table_name}")
        os.makedirs(logs_dir, exist_ok=True)
        Path(logs_dir).mkdir(parents=True, exist_ok=True)
        logging.info(f"Logs directory created: {logs_dir}")

        table_utils = get_table_utils(table_name)
        logging.info(f"Retrieved table utilities: \n{table_utils}, {type(table_utils)}")

        runtime_params = {
            "table_name": table_name,
            "columns": columns,
            "aod": aod,
            "s3_bucket_name": s3_bucket_name,
            "s3_dir_path": s3_dir_path,
            "local_output_dir": local_output_dir,
            "csv_file_name": csv_file_name,
            "zip_file_name": zip_file_name,
            "csv_out_path": csv_out_path,
            "logs_dir": logs_dir,
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
                DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
                WHERE CAST(DATE_ENTERED AS DATE) = TO_DATE('{aod}');
            """

        elif table_filtered_by and table_filtered_by != 'NULL':
            sfsql_query = f"""
                DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
                WHERE ASOFDATE = TO_DATE('{aod}');
            """

        elif key_column and key_column != 'NULL':
            s3_path = f"{s3_dir_path}/{zip_file_name}"
            sfsql_query = f"""
                DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
                WHERE METADATAFILENAME = 'inbound/{s3_path}';
            """

        else:
            sfsql_query = f"""
                DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
                WHERE ASOFDATE = TO_DATE('{aod}');
            """

        logging.info(f"Snowflake SQL query: \n{sfsql_query}")
        if not sfsql_query:
            raise ValueError(f"No Snowflake SQL query generated. Check the reset snowflake task for issues.")
        
        get_sf_hook().run(sfsql_query)
        logging.info(f"Successfully executed Snowflake query")


    @task
    def run_bcp(runtime_params, delimiter='|', linebreak=os.linesep):
        """
        Fetches the MSSQL query "mssql_query" and writes the exported data to a CSV file.
        
        Executes the BCP utility command to export data from the MSSQL database.

        Parameters:
            delimiter (str, optional): The delimiter to use in the CSV file. Defaults to '|'.
            linebreak (str, optional): The linebreak character in the CSV file. Defaults to os.linesep.

        Raises:
            Exception: If the CSV file is not found after the export.
        """
        
        logging.info(f"Running BCP task")

        table_name = runtime_params.get("table_name")
        aod = runtime_params.get("aod")
        csv_out_path = runtime_params.get("csv_out_path")
        logs_dir = runtime_params.get("logs_dir")
        
        logging.info(f"Retrieving MSSQL query")

        ## Get the MSSQL query based on the runtime parameters
        table_utils = runtime_params.get("table_utils")
        sql_file = table_utils.get('SQL')
        table_filtered_by = table_utils.get('TABLE_FILTERED_BY')
        key_column = table_utils.get('KEY_COLUMN')
        
        ## Assign the query to empty string for error catching later
        mssql_query = '' 
        
        # If there's a custom SQL file...
        if sql_file and sql_file != 'NULL':
            with open(f"{SQL_DIR}/{sql_file}", "r") as file:
                mssql_query = file.read().format(aod)
        
        # If there's a date column to filter by...
        elif table_filtered_by and table_filtered_by != 'NULL':
            mssql_query = f"""SELECT * FROM [dbo].[{table_name}] WHERE CAST([{table_filtered_by}] AS DATE) = '{aod}'"""
        
        # If there's a key column to filter by...
        elif key_column and key_column != 'NULL':
            snowflake_query = f"""
                SELECT MAX({key_column})
                FROM STG.{DATABASE_NAME}_{table_name}_HIST
            """
            max_value_df = get_sf_hook().get_pandas_df(snowflake_query)
            max_value = max_value_df.iloc[0, 0]
            max_value = 0 if max_value is None else max_value
            mssql_query = f"""SELECT * FROM [dbo].[{table_name}] WHERE [{key_column}] > {max_value}"""
        
        # If none of the above are true...
        else:
            mssql_query = f"SELECT * FROM [dbo].[{table_name}]"

        logging.info(f"Microsoft SQL Server query: \n{mssql_query}")
        if not mssql_query:
            raise ValueError(f"No Microsoft SQL query generated. Check the BCP task for issues.")

        ## Create CSV for BCP to write to (delete/clear any existing files)
        if os.path.exists(csv_out_path):
            os.remove(csv_out_path) # Delete the file
            logging.info(f"Found existing CSV file in local directory, deleting {csv_out_path}")
            # Path(csv_out_path).unlink()
        Path(csv_out_path).touch(exist_ok=True)

        ## Create Log files to export the BCP command line output
        log_path = os.path.join(logs_dir, f'BCP_{DATABASE_NAME}_{table_name}_Log_File.log')
        err_path = os.path.join(logs_dir, f'BCP_{DATABASE_NAME}_{table_name}_Err_File.log')
        Path(log_path).touch(exist_ok=True)
        Path(err_path).touch(exist_ok=True)

        ## Connect to the MSSQL database using the Airflow connection
        engine = get_mssql_hook().get_sqlalchemy_engine()
        server = engine.url.host
        username = engine.url.username
        password = engine.url.password
        logging.info(f"Retrieved MSSQL database connection variables: \nEngine: {engine} \nServer: {server} \nUsername: {username}")
    
        ## Construct the BCP command
        bcp_cmd = [
            "/opt/mssql-tools/bin/bcp",
            f"{mssql_query}",
            "queryout", csv_out_path, 
            "-c",
            "-t", delimiter,
            "-r", linebreak,
            "-T", 
            "-S", server,  # Use the host from the connection
            "-d", DATABASE_NAME,
            "-u", username,
            "-p", password,
            "-o", log_path, # output_file
            "-e", err_path, # err_file
        ]
        logging.info(f"Running BCP utility: {bcp_cmd}")

        # Execute the BCP command using the subprocess module
        subprocess.run(bcp_cmd, stderr=subprocess.PIPE)

        if not os.path.exists(csv_out_path):
            raise ValueError(f"BCP run unsuccessful. CSV not found: {csv_out_path}")
        
        logging.info(f"Successfully executed BCP command and exported data to {csv_out_path} \nFile size is {os.path.getsize(csv_out_path)} bytes")

        return csv_out_path


    @task
    def run_pigz(runtime_params): 
        """
        Compresses the CSV file obtained from the previous task using the PigZ utility.

        Raises:
            ValueError: If any errors occur while running the PigZ subprocess or if the ZIP file is not found.
        """
        logging.info(f"Running PigZ")

        # Get the name of the CSV file at the end of the file path 
        csv_out_path = runtime_params.get("csv_out_path")
        zip_path = csv_out_path + '.gz'
        
        if not os.path.exists(csv_out_path):
            raise ValueError(f"Unable to run PigZ. CSV not found: {csv_out_path}")
        
        if os.path.exists(zip_path):
            logging.info(f"Found existing zip file in local directory, deleting {zip_path}")
            # Delete the file
            os.remove(zip_path)
            # Path(zip_path).unlink()
        
        # Run pigz to compress the CSV file in the current directory
        pigz_cmd = f'pigz {csv_out_path}'.split(maxsplit=1)
        proc = subprocess.Popen( 
            pigz_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logging.info(f"Running PigZ: {pigz_cmd}")

        # Log any errors when running PigZ subprocess
        out, err = proc.communicate()
        if err != b'':
            raise ValueError(f'The following error occured while running pigZ: {err}')
        
        # Check the zip file was created with the correct name
        if os.path.exists(zip_path) is False:
            raise ValueError(f"Zip file does not exist: {zip_path}")
        
        logging.info(f"Successfully zipped CSV file with PigZ: {zip_path}")

        return zip_path


    @task
    def load_to_s3(runtime_params):
        """
        Loads the ZIP file obtained from the previous task to the specified S3 bucket.
        """
        csv_out_path = runtime_params.get("csv_out_path")
        zip_file_name = runtime_params.get("zip_file_name")
        s3_bucket_name = runtime_params.get("s3_bucket_name")
        s3_dir_path = runtime_params.get("s3_dir_path")

        # Load zip file to S3
        get_s3_hook().load_file(
            filename=csv_out_path+'.gz',
            bucket_name=s3_bucket_name,
            replace=True,
            key=f"inbound/{s3_dir_path}/{zip_file_name}",
        )

        # Delete all the files created locally
        # local_output_dir = runtime_params.get("local_output_dir")
        # shutil.rmtree(local_output_dir)

        # shutil.rmtree('include/bcp')
        # shutil.rmtree('include/logs')
        
        # os.remove(csv_out_path) # delete file
        # os.removedirs(local_output_dir) # delete folders


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
            -- \\ {DATABASE_NAME}_{table_name}_HIST
            COPY INTO STG.{DATABASE_NAME}_{table_name}_HIST 
            FROM (
                SELECT 
                    METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{previous_day}'), 
                    {columns} 
                FROM @ETL.INBOUND/{s3_dir_path}/
            )
            FILE_FORMAT = ( 
                {FILE_FORMAT}
            ) 
            PATTERN = '.*{zip_file_name}.*'
        """
        get_sf_hook().run(sql_query)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule="all_done")

    # trigger_DQ = TriggerDagRunOperator(
    # 	task_id=f"trigger_DQ_{DATABASE_NAME}",
    # 	trigger_dag_id=f"DQ_{DATABASE_NAME}",
    # 	reset_dag_run=True,
    # 	wait_for_completion=True,
    # 	poke_interval=30,
    # 	execution_date='{{ ds }}',
    # 	# dag=Staging_CREO_BCP
    # )
    
    for table_name in fullTableList: 
        with TaskGroup(group_id=f"{DATABASE_NAME}_{table_name}_HIST_Task") as export_mssql_load_snowflake:
            runtime_params = get_runtime_params(table_name, ds="2023-07-01")
            # runtime_params

            reset_snowflake_task = reset_snowflake(runtime_params)
            # reset_snowflake_task
            
            run_bcp_task = run_bcp(runtime_params)
            # reset_snowflake_task >> run_bcp_task
            
            run_pigz_task = run_pigz(runtime_params)
            # reset_snowflake_task >> run_bcp_task >> run_pigz_task

            load_to_s3_task = load_to_s3(runtime_params)
            # reset_snowflake_task >> run_bcp_task >> run_pigz_task >> load_to_s3_task
            
            copy_s3_to_snowflake_task = copy_s3_to_snowflake(runtime_params)
            reset_snowflake_task >> run_bcp_task >> run_pigz_task >> load_to_s3_task >> copy_s3_to_snowflake_task
            
    start >> export_mssql_load_snowflake >> end #>> trigger_DQ

staging_creo = Staging_CREO_BCP()