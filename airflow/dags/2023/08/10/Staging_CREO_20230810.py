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
	"APPROVALREQUEST", "APPROVALREQUESTITEM", "CAMPAIGN", "CAMPAIGNTYPE", "COMMUNICATION", "COMMUNICATIONMAILING", "CONFIG", "CONFIGHISTORY", "CONTACT", "CONTACTTYPE", "CONTAINER", "DATASET", "DATASETCELL", "DATASETCOLUMN", "DATASETROW", "DATASETVALUE", "DATASOURCE", "DEADMESSAGES", "DEADMESSAGES2", "DELIVERYSTATUS", "EMOJI", "FOLDER", "FOLDERCONTACT", "FOLDERMESSAGE", "GLOBAL", "LOG", "MESSAGE", "MESSAGECONTACT", "MESSAGECONTACTTYPE", "MESSAGECONTACTV2", "MESSAGEDELIVERYSTATUS", "MESSAGEPART", "MESSAGEPARTV2", "MESSAGESTATUSQUEUE", "MESSAGETYPE", "PACKAGE", "PACKAGETEMPLATE", "PARAMETER", "RULE", "TEMPLATE", "TEMPLATERULE", "TEMPLATETYPE", "TEMPMESSAGE", "USER", "WEBHOOK"
]

# Constants >> Defined in upper case to distinguish from locally defined variables
DATABASE_NAME = "CREO"
FILE_FORMAT = "FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ"
SQL_DIR = f"/usr/local/airflow/include/sql/Staging_{DATABASE_NAME}"
START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')
TESTING = False


# - - - - - - CONNECTIONS & HOOKS - - - - - - -

# MSSQL_CONN = "awsmssql_creosql_creo_conn"
# MSSQL_HOOK = MsSqlHook(conn_id=MSSQL_CONN)

# S3_CONN = "aws_s3_conn"
# S3_HOOK = S3Hook(aws_conn_id=S3_CONN)

# SF_CONN = "snowflake_default"
# SF_HOOK = SnowflakeHook(conn_id=SF_CONN)


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
	# dfTableList = get_sf_hook()\
	# 	.get_pandas_df(f"""
	# 	 	SELECT * FROM ARES.ETL.{DATABASE_NAME}_UTILS
	# 	""")
	# logging.info(f"dfTableList = {dfTableList}, {type(dfTableList)}")

	# table_list = dfTableList["TABLE_NAME"].to_list()
	# logging.info(f"table_list = {table_list}, {type(table_list)}")
	utils_query = f"""
		 	SELECT * FROM ARES.ETL.{DATABASE_NAME}_UTILS
			WHERE table_name = '{DATABASE_NAME}_{table_name}_HIST'
		"""
	table_utils = get_sf_hook().get_first(utils_query)
	
	return table_utils


# - - - - DYNAMIC MS & SF QUERY FUNCTIONS - - - -

def use_sql_file(aod, sql_file=None, table_name=None):
	"""
	Reads and returns the SQL query from the specified SQL file.

	Returns:
		str: The SQL query with the provided date formatted in the file.
	"""
	logging.info(f"Found custom SQL file")
	
	if table_name:
		sfsql_query = f"""
			DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
			WHERE CAST(DATE_ENTERED AS DATE) = TO_DATE('{aod}');
		"""
		logging.info(f"Snowflake SQL query: \n{sfsql_query}")
		return sfsql_query
	
	if sql_file:
		with open(f"{SQL_DIR}/{sql_file}", "r") as file:
			mssql_query = file.read().format(aod)
		logging.info(f"Microsoft SQL Server query: \n{mssql_query}")
		return mssql_query


def use_date_column(aod, table_name=None, table_filtered_by=None):
	"""
	Generates and returns the SQL query to select data based on the provided date column.

	Returns:
		str: The SQL query to select data based on the provided date column and date.
	"""
	logging.info(f"Found date column to filter by")
	
	if table_filtered_by:
		mssql_query = f"""
			SELECT * FROM [dbo].[{table_name}]
			WHERE CAST([{table_filtered_by}] AS DATE) = '{aod}';
		"""
		logging.info(f"Microsoft SQL Server query: \n{mssql_query}")
		return mssql_query
	
	else:
		sfsql_query = f"""
			DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
			WHERE ASOFDATE = TO_DATE('{aod}');
		"""
		logging.info(f"Snowflake SQL query: \n{sfsql_query}")
		return sfsql_query
	


def use_key_column(table_name=None, key_column=None, s3_path=None):
	"""
	Generates and returns the SQL query to select data based on the provided key column.

	Returns:
		str: The SQL query to select data based on the provided key column.
	"""
	
	logging.info(f"Found key column to filter by")
	
	if s3_path:
		sfsql_query = f"""
				DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
				WHERE METADATAFILENAME = 'inbound/{s3_path}';
			"""
		logging.info(f"Snowflake SQL query: \n{sfsql_query}")
		return sfsql_query
	
	if key_column:
		snowflake_query = f"""
			SELECT MAX({key_column})
			FROM STG.{DATABASE_NAME}_{table_name}_HIST
		"""
		max_value_df = get_sf_hook().get_pandas_df(snowflake_query)
		max_value = max_value_df.iloc[0, 0]

		if max_value is None:
			max_value = 0
		# print(max_value)

		mssql_query = f"""
			SELECT * 
			FROM [dbo].[{table_name}]
			WHERE [{key_column}] > {max_value};
		"""
		logging.info(f"Microsoft SQL Server query: \n{mssql_query}")
		return mssql_query


def no_sql_date_keycol(table_name=None, aod=None):
	"""
	Generates and returns the SQL query to export all data from the specified table.

	Returns:
		str: The SQL query to export all data from the specified table.
	"""
	logging.info(f"Did not find a custom SQL file, date column to filter by, nor primary key column")

	if aod:
		sfsql_query = f"""
			DELETE FROM STG.{DATABASE_NAME}_{table_name}_HIST 
			WHERE ASOFDATE = TO_DATE('{aod}');
		"""
		logging.info(f"Snowflake SQL query: \n{sfsql_query}")
		return sfsql_query

	else:
		mssql_query = f"SELECT * FROM [dbo].[{table_name}]"
		logging.info(f"Microsoft SQL Server query: \n{mssql_query}")
		return mssql_query


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
	"Staging_CREO",
	start_date=START_DATE,
	catchup=False,
	schedule='0 6 * * *',  # 6AM EST
	dagrun_timeout=timedelta(hours=4),
	doc_md=__doc__,
	template_searchpath=f"include/sql/Staging_{DATABASE_NAME}/",
	default_args=default_args
)
def Staging_CREO():

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
			- local_output_dir (str): The path to the local directory where the CSV will be exported after BCP.
			- table_list (dict): 
		"""

		query_output = get_sf_hook().get_first(f"""SELECT ETL.COPYSELECT('STG','{DATABASE_NAME}_{table_name}_HIST',3)""")
		columns = query_output[f"ETL.COPYSELECT('STG','{DATABASE_NAME}_{table_name}_HIST',3)"]
		aod = datetime.strptime(ds, "%Y-%m-%d")
		s3_bucket_name = Variable.get("s3_etldata_bucket_var")
		s3_dir_path = aod.strftime(f"{DATABASE_NAME}/%Y/%m/%d/{table_name}")
		csv_file_name = aod.strftime(f"{table_name}_%Y%m%d.csv")
		zip_file_name = csv_file_name + '.gz'
		local_output_dir = aod.strftime(f"include/bcp/Staging_{DATABASE_NAME}/%Y/%m/%d/{table_name}")
		# Create the directory and any missing parent directories
		os.makedirs(local_output_dir, exist_ok=True)
		# Path(local_output_dir).mkdir(parents=True, exist_ok=True)

		# table_list = runtime_params.get("table_utils")

		table_utils = get_table_utils(table_name)
		logging.info(f"Table utils = {table_utils}, {type(table_utils)}")

		runtime_params = {
			"table_name": table_name,
			"columns": columns,
			"aod": aod,
			"s3_bucket_name": s3_bucket_name,
			"s3_dir_path": s3_dir_path,
			"csv_file_name": csv_file_name,
			"zip_file_name": zip_file_name,
			"local_output_dir": local_output_dir,
			"table_utils": table_utils,
		}

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

		table_list = runtime_params.get("table_utils")
		if table_list:
			sql_file = table_list.get("sql".upper())
			table_filtered_by = table_list.get("table_filtered_by".upper())
			key_column = table_list.get("key_column".upper())

		# if sql_file != 'NULL':
		# 	sf_sql = use_sql_file(aod=aod, sql_file=sql_file, sf_table_name=sf_table_name)

		if table_filtered_by != 'NULL':
			sf_sql = use_date_column(aod=aod, table_name=table_name)

		elif key_column != 'NULL':
			s3_path = f"{s3_dir_path}/{zip_file_name}"
			sf_sql = use_key_column(table_name=table_name, s3_path=s3_path)

		else:
			sf_sql = no_sql_date_keycol(table_name=table_name, aod=aod)

		get_sf_hook().run(sf_sql)
		logging.info(f"Successfully executed Snowflake query")


	@task
	def run_bcp(runtime_params, delimiter='|', linebreak='\\n'):
		"""
		Fetches the MSSQL query "mssql_query" and writes the exported data to a CSV file.
		
		Executes the BCP utility command to export data from the MSSQL database.

		Parameters:
			delimiter (str, optional): The delimiter to use in the CSV file. Defaults to '|'.
			linebreak (str, optional): The linebreak character in the CSV file. Defaults to os.linesep.

		Raises:
			Exception: If the CSV file is not found after the export.
		"""
		logging.info(f"Running BCP command")

		table_name = runtime_params.get("table_name")
		aod = runtime_params.get("aod")
		
		# 
		table_list = runtime_params.get("table_utils")
		if table_list:
			sql_file = table_list.get("sql".upper())
			table_filtered_by = table_list.get("table_filtered_by".upper())
			key_column = table_list.get("key_column".upper())

		## Get the MSSQL query based on the runtime parameters
		# if sql_file != 'NULL':
		# 	mssql_query = use_sql_file(aod=aod, sql_file=sql_file)

		if table_filtered_by != 'NULL':
			mssql_query = use_date_column(aod=aod, table_name=table_name, table_filtered_by=table_filtered_by)

		elif key_column != 'NULL':
			mssql_query = use_key_column(table_name=table_name, key_column=key_column)

		else:
			mssql_query = no_sql_date_keycol(table_name=table_name)

		local_output_dir = runtime_params.get("local_output_dir")
		csv_file_name = runtime_params.get("csv_file_name")
		csv_out_path = os.path.join(local_output_dir, csv_file_name)
		Path(csv_out_path).touch(exist_ok=True)
		logging.info(f"Local CSV file created: {csv_out_path}")

		# # create logs and error files
		logs_dir = datetime.now().strftime(f"include/logs/Staging_{DATABASE_NAME}/%Y/%m/%d/{table_name}")
		os.makedirs(logs_dir, exist_ok=True)
		# Path(logs_dir).mkdir(parents=True, exist_ok=True)

		log_path = os.path.join(logs_dir, f'BCP_{DATABASE_NAME}_{table_name}_Log_File')
		err_path = os.path.join(logs_dir, f'BCP_{DATABASE_NAME}_{table_name}_Err_File')
		Path(log_path).touch(exist_ok=True)
		Path(err_path).touch(exist_ok=True)
		logging.info(f"BCP logs saved at {logs_dir}")

		# Connect to the MSSQL database using the Airflow connection
		engine = get_mssql_hook().get_sqlalchemy_engine()
		server = engine.url.host
		username = engine.url.username
		password = engine.url.password
		logging.info("Retrieved MSSQL database connection variables: \n"
	       f"Engine: {engine} \n"
		   f"Server: {server} \n"
		   f"Username: {username}"
		)
	
		# Construct the BCP command
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
		logging.info(f"Running BCP: {bcp_cmd}")

		# Execute the BCP command using the subprocess module
		subprocess.run(bcp_cmd, stderr=subprocess.PIPE)
		logging.info(f"Successfully executed BCP command and exported data to {csv_out_path}")
		logging.info(f">> File size is {os.path.getsize(csv_out_path)} bytes")
		
		# return csv_out_path


	@task
	def run_pigz(runtime_params): 
		"""
		Compresses the CSV file obtained from the previous task using the PigZ utility.

		Raises:
			ValueError: If any errors occur while running the PigZ subprocess or if the ZIP file is not found.
		"""
		logging.info(f"Running PigZ")

		# Get the name of the CSV file at the end of the file path 
		local_output_dir = runtime_params.get("local_output_dir")
		csv_file_name = runtime_params.get("csv_file_name")
		csv_out_path = os.path.join(local_output_dir, csv_file_name)
		zip_file_name = runtime_params.get("zip_file_name")
		zip_path = os.path.join(local_output_dir, zip_file_name)
		
		if os.path.exists(zip_path):
			logging.info(f"Found zip file in local directory, deleting existing file: {zip_path}")
			# os.remove(zip_path)
			# Delete the file
			Path(zip_path).unlink()
		
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

		# return zip_path

	@task
	def load_zip_to_s3(runtime_params):
		"""
		Loads the ZIP file obtained from the previous task to the specified S3 bucket.
		"""

		local_output_dir = runtime_params.get("local_output_dir")
		zip_file_name = runtime_params.get("zip_file_name")
		local_zip_path = os.path.join(local_output_dir, zip_file_name)

		s3_bucket_name = runtime_params.get("s3_bucket_name")
		s3_dir_path = runtime_params.get("s3_dir_path")

		# Load zip file to S3
		get_s3_hook().load_file(
			filename=local_zip_path,
			bucket_name=s3_bucket_name,
			replace=True,
			key=f"inbound/{s3_dir_path}/{zip_file_name}",
		)

		# Delete all the files created locally
		shutil.rmtree('include/bcp')
		shutil.rmtree('include/logs')
		# os.remove(local_zip_path)
		# os.rmdir(os.path.dirname(local_zip_path))


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
	# 	# dag=Staging_CREO
	# )
	
	for table_name in fullTableList: 
		with TaskGroup(group_id=f"{DATABASE_NAME}_{table_name}_HIST_Task") as export_mssql_load_snowflake:
			runtime_params = get_runtime_params(table_name, ds="2023-07-01")
			# runtime_params

			reset_snowflake_task = reset_snowflake(runtime_params)
			# reset_snowflake_task
			
			run_bcp_task = run_bcp(runtime_params)
			# run_bcp_task
			
			# run_pigz_task = run_pigz(runtime_params)
			# run_pigz_task

			# load_zip_to_s3_task = load_zip_to_s3(runtime_params)
			# load_zip_to_s3_task
			
			# copy_s3_to_snowflake_task = copy_s3_to_snowflake(runtime_params)
			# copy_s3_to_snowflake_task

			reset_snowflake_task >> run_bcp_task #>> run_pigz_task >> load_zip_to_s3_task >> copy_s3_to_snowflake_task

	start >> export_mssql_load_snowflake >> end #>> trigger_DQ

staging_creo = Staging_CREO()
