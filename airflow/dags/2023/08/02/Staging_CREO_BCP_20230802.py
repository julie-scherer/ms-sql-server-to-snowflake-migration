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

fullTableList = {
	"CREO_APPROVALREQUEST_HIST": {
		"sql": None,
		"table_filtered_by": "ENTERED_AT",
		"key_column": "APPROVAL_REQUEST_KEY"
	},
	"CREO_APPROVALREQUESTITEM_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "APPROVAL_REQUEST_ITEM_KEY"
	},
	"CREO_CAMPAIGN_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "CAMPAIGN_KEY"
	},
	# "CREO_CAMPAIGNTYPE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "CAMPAIGN_TYPE_KEY"
	# },
	# "CREO_COMMUNICATION_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "COMMUNICATION_KEY"
	# },
	# "CREO_COMMUNICATIONMAILING_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_START",
	# 	"key_column": "COMMUNICATION_MAILING_KEY"
	# },
	# "CREO_CONFIG_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "CONFIG_KEY"
	# },
	# "CREO_CONFIGHISTORY_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "CONFIG_HISTORY_KEY"
	# },
	# "CREO_CONTACT_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "CONTACT_KEY"
	# },
	# "CREO_CONTACTTYPE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "CONTACT_TYPE_KEY"
	# },
	# "CREO_CONTAINER_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "CONTAINER_KEY"
	# },
	# "CREO_DATASET_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "DATASET_KEY"
	# },
	# "CREO_DATASETCELL_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_DATASETCOLUMN_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "DATASET_COLUMN_KEY"
	# },
	# "CREO_DATASETROW_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "DATASET_ROW_KEY"
	# },
	# "CREO_DATASETVALUE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "DATASET_VALUE_KEY"
	# },
	# "CREO_DATASOURCE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "DATASOURCE_KEY"
	# },
	# "CREO_DEADMESSAGES_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": None
	# },
	# "CREO_DEADMESSAGES2_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": None
	# },
	# "CREO_DELIVERYSTATUS_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "DELIVERY_STATUS_KEY"
	# },
	# "CREO_EMOJI_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "EMOJI_KEY"
	# },
	# "CREO_FOLDER_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "FOLDER_KEY"
	# },
	# "CREO_FOLDERCONTACT_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_FOLDERMESSAGE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_GLOBAL_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_LOG_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "LOG_KEY"
	# },
	# "CREO_MESSAGE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "MESSAGE_KEY"
	# },
	# "CREO_MESSAGECONTACT_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "MESSAGE_CONTACT_KEY"
	# },
	# "CREO_MESSAGECONTACTTYPE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "MESSAGE_CONTACT_TYPE_KEY"
	# },
	# "CREO_MESSAGECONTACTV2_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_MESSAGEDELIVERYSTATUS_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "MESSAGE_DELIVERY_STATUS_KEY"
	# },
	# "CREO_MESSAGEPART_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "MESSAGE_PART_KEY"
	# },
	# "CREO_MESSAGEPARTV2_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_MESSAGESTATUSQUEUE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "ENTERED_AT",
	# 	"key_column": "MESSAGE_STATUS_QUEUE_KEY"
	# },
	# "CREO_MESSAGETYPE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "MESSAGE_TYPE_KEY"
	# },
	# "CREO_PACKAGE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "PACKAGE_KEY"
	# },
	# "CREO_PACKAGETEMPLATE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_PARAMETER_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "PARAMETER_KEY"
	# },
	# "CREO_RULE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "RULE_KEY"
	# },
	# "CREO_TEMPLATE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "TEMPLATE_KEY"
	# },
	# "CREO_TEMPLATERULE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": None
	# },
	# "CREO_TEMPLATETYPE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "TEMPLATE_TYPE_KEY"
	# },
	# "CREO_TEMPMESSAGE_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": None,
	# 	"key_column": "TEMP_MESSAGE_KEY"
	# },
	# "CREO_USER_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": "USER_KEY"
	# },
	# "CREO_WEBHOOK_HIST": {
	# 	"sql": None,
	# 	"table_filtered_by": "DATE_ENTERED",
	# 	"key_column": None
	# }

}

# Constants >> Defined in upper case to distinguish from locally defined variables
DATABASE_NAME = "CREO"
FILE_FORMAT = "STG.LD_CSV_PIPE_SH1_EON_GZ"
SQL_DIR = f"/usr/local/airflow/include/sql/Staging_{DATABASE_NAME}"
START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')
TESTING = False

MSSQL_CONN = "awsmssql_creosql_creo_conn"
MSSQL_HOOK = MsSqlHook(conn_id=MSSQL_CONN)

S3_CONN = "aws_s3_conn"
S3_HOOK = S3Hook(aws_conn_id=S3_CONN)

SF_CONN = "snowflake_default"
SF_HOOK = SnowflakeHook(conn_id=SF_CONN)



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



def use_sql_file(aod, sql_file):
	"""
	Reads and returns the SQL query from the specified SQL file.

	Parameters:
		aod (datetime): The date as a datetime object.
		sql_file (str): The name of the SQL file to read the query from.

	Returns:
		str: The SQL query with the provided date formatted in the file.
	"""
	with open(f"{SQL_DIR}/{sql_file}", "r") as file:
		mssql_query = file.read().format(aod)
		print(mssql_query)
	return mssql_query

def use_date_column(aod, trunc_table_name, table_filtered_by):
	"""
	Generates and returns the SQL query to select data based on the provided date column.

	Parameters:
		aod (datetime): The date as a datetime object.
		trunc_table_name (str): The truncated table name without database prefix and "_HIST".
		table_filtered_by (str): The name of the date column to filter data.

	Returns:
		str: The SQL query to select data based on the provided date column and date.
	"""
	mssql_query = f"""
		SELECT * FROM dbo.{trunc_table_name} 
		WHERE CAST({table_filtered_by} AS DATE) = '{aod}';
	"""
	return mssql_query

def use_key_column(sf_table_name, trunc_table_name, key_column):
	"""
	Generates and returns the SQL query to select data based on the provided key column.

	Parameters:
		sf_table_name (str): The name of the Snowflake table.
		trunc_table_name (str): The truncated table name without database prefix and "_HIST".
		key_column (str): The name of the key column to filter data.

	Returns:
		str: The SQL query to select data based on the provided key column.
	"""
	snowflake_query = f"""
		SELECT MAX({key_column})
		FROM STG.{sf_table_name}
	"""
	max_value_df = get_sf_hook().get_pandas_df(snowflake_query)
	max_value = max_value_df.iloc[0, 0]

	if max_value is None:
		max_value = 0
	print(max_value)

	mssql_query = f"""
		SELECT * 
		FROM dbo.{trunc_table_name} 
		WHERE {key_column} > {max_value};
	"""
	return mssql_query

def no_sql_date_keycol(trunc_table_name):
	"""
	Generates and returns the SQL query to export all data from the specified table.

	Parameters:
		trunc_table_name (str): The truncated table name without database prefix and "_HIST".

	Returns:
		str: The SQL query to export all data from the specified table.
	"""
	mssql_query = f"SELECT * FROM dbo.{trunc_table_name}"
	return mssql_query



## Default DAG args
default_args = {
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
	schedule_interval='0 6 * * *',  # 6AM EST
	dagrun_timeout=timedelta(hours=4),
	doc_md=__doc__,
	template_searchpath=f"include/sql/Staging_{DATABASE_NAME}/",
	default_args=default_args
)
def Staging_CREO():

	@task(multiple_outputs=True)
	def get_runtime_params(sf_table_name, ds=None) -> dict:
		"""
		Fetches runtime parameters and sets them in the context dictionary.
		Retrieves the MSSQL query based on runtime parameters and data configurations.
		The query is determined based on various conditions, including the presence
		of an SQL file, a date column, or a key column.

		Parameters:
			sf_table_name (str): The name of the Snowflake table to export data to.
			ds (str, optional): The date string in the format "YYYY-MM-DD". Defaults to None.

		Runtime parameters:
			- sf_table_name (str): The name of the Snowflake table.
			- trunc_table_name (str): The truncated table name without database prefix and "_HIST".
			- columns (str): The columns to be copied.
			- aod (datetime): The date as a datetime object.
			- s3_file_path (str): The S3 bucket file path where the data will be stored.
			- csv_file_name (str): The name of the CSV file to be created.
			- zip_file_name (str): The name of the ZIP file to be created.
			- local_output_dir (str): The path to the local directory where the CSV will be exported after BCP.
			- mssql_query (str): The MSSQL query based on the presence of an SQL file, a date column, or a key column
			- table_list (dict): 
		"""
		query_output = get_sf_hook().get_first(f"""
	      SELECT ETL.COPYSELECT('STG','{sf_table_name.upper()}',3)
		""")

		columns = query_output[f"ETL.COPYSELECT('STG','{sf_table_name.upper()}',3)"]
		aod = datetime.strptime(ds, "%Y-%m-%d")
		trunc_table_name = sf_table_name.replace(f"{DATABASE_NAME}_","").replace("_HIST","")
		s3_file_path = aod.strftime(f"{DATABASE_NAME}/%Y/%m/{trunc_table_name}/")
		csv_file_name = aod.strftime(f"{trunc_table_name}_%Y%m%d.csv")
		zip_file_name = csv_file_name + '.gz'
		local_output_dir = datetime.now().strftime(f"include/bcp/Staging_{DATABASE_NAME}/%Y/%m/%d/{trunc_table_name}")
		# runtime_params = {
		# 	"sf_table_name": sf_table_name,
		# 	"trunc_table_name": trunc_table_name,
		# 	"columns": columns,
		# 	"aod": aod,
		# 	"s3_file_path": s3_file_path,
		# 	"csv_file_name": csv_file_name,
		# 	"zip_file_name": zip_file_name,
		# 	"local_output_dir": local_output_dir,
		# }

		
		table_list = fullTableList.get(sf_table_name)
		if table_list:
			sql_file = table_list.get("sql")
			table_filtered_by = table_list.get("table_filtered_by")
			key_column = table_list.get("key_column")

		if sql_file:
			mssql_query = use_sql_file(aod, sql_file)

		elif table_filtered_by:
			mssql_query = use_date_column(aod, trunc_table_name, table_filtered_by)

		elif key_column:
			mssql_query = use_key_column(sf_table_name, trunc_table_name, key_column)

		else:
			mssql_query = no_sql_date_keycol(trunc_table_name)

		runtime_params = {
			"sf_table_name": sf_table_name,
			"trunc_table_name": trunc_table_name,
			"columns": columns,
			"aod": aod,
			"s3_file_path": s3_file_path,
			"csv_file_name": csv_file_name,
			"zip_file_name": zip_file_name,
			"local_output_dir": local_output_dir,
			"mssql_query": mssql_query,
			"table_list": table_list,
		}

		return runtime_params


	@task
	def reset_snowflake(runtime_params):
		"""
		Resets the data in the specified Snowflake table based on the provided parameters.

		Parameters:
			sf_table_name (str): The name of the Snowflake table.
			aod (datetime): The date as a datetime object.
			s3_path (str, optional): The S3 file path. Defaults to None.
		"""
		sf_table_name = runtime_params.get("sf_table_name")
		s3_file_path = runtime_params.get("s3_file_path")
		zip_file_name = runtime_params.get("zip_file_name")
		aod = runtime_params.get("aod")

		table_list = fullTableList.get(sf_table_name)
		if table_list:
			sql_file = table_list.get("sql")
			table_filtered_by = table_list.get("table_filtered_by")
			key_column = table_list.get("key_column", False)

		if sql_file:
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE CAST(DATE_ENTERED AS DATE) = TO_DATE('{aod}');
			"""

		elif table_filtered_by:
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE ASOFDATE = TO_DATE('{aod}');
			"""

		elif key_column:
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE FILENAME = '{s3_file_path}/{zip_file_name}';
			"""

		else:
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE ASOFDATE = TO_DATE('{aod}');
			"""
		get_sf_hook().run(sf_sql)


	@task
	def run_bcp(runtime_params, delimiter='|', linebreak=os.linesep):
		"""
		Executes the BCP utility command to export data from the MSSQL database.

		Fetches the SQL query from the XCom variable "mssql_query" and writes the exported data
		to a CSV file.

		Parameters:
			csv_out_path (str): The file path to save the exported CSV file.
			query (str): The SQL query to execute for data export.
			delimiter (str, optional): The delimiter to use in the CSV file. Defaults to '|'.
			linebreak (str, optional): The linebreak character in the CSV file. Defaults to os.linesep.

		Raises:
			Exception: If the CSV file is not found after the export.
		"""

		# mssql_query = runtime_params.get("mssql_query")

		trunc_table_name = runtime_params.get("trunc_table_name")
		aod = runtime_params.get("aod")
		
		table_list = fullTableList.get(sf_table_name)

		if table_list:
			sql_file = table_list.get("sql")
			table_filtered_by = table_list.get("table_filtered_by")
			key_column = table_list.get("key_column")

		if sql_file:
			mssql_query = use_sql_file(aod, sql_file)

		elif table_filtered_by:
			mssql_query = use_date_column(aod, trunc_table_name, table_filtered_by)

		elif key_column:
			mssql_query = use_key_column(sf_table_name, trunc_table_name, key_column)

		else:
			mssql_query = no_sql_date_keycol(trunc_table_name)

		csv_file_name = runtime_params.get("csv_file_name")
		local_output_dir = runtime_params.get("local_output_dir")
		
		os.makedirs(local_output_dir, exist_ok=True)
		csv_out_path = os.path.join(local_output_dir, csv_file_name)

		# Connect to the MSSQL database using the Airflow connection
		conn = get_mssql_hook().get_conn()
		server = get_mssql_hook().get_sqlalchemy_engine().url.host

		logging.info(f"BCP connection: {conn}")
		logging.info(f"BCP server: {server}")
		logging.info(f"BCP query: {mssql_query}")

		# Construct the BCP command
		bcp_cmd = [
			"/opt/mssql-tools/bin/bcp",
			f"{mssql_query}",
			"queryout", csv_out_path, 
			"-c",
			"-t" + delimiter,
			"-r" + linebreak,
			"-T", 
			"-S", server,  # Use the host from the connection
			"-d", DATABASE_NAME
		]

		# Execute the BCP command inside the MSSQL connection
		cursor = conn.cursor()
		cursor.execute(" ".join(bcp_cmd))
		cursor.close()
		conn.close()

		# Check the CSV was exported
		if not Path.is_file(csv_out_path):
			raise Exception(f'File not found: {csv_out_path}')

		logging.info(f"Successfully executed BCP: {csv_out_path}")
		logging.info(f">> File Size: {os.path.getsize(csv_out_path)} bytes")
		logging.info(f">> Path: {csv_out_path}")
		
		# return local_output_dir


	@task
	def run_pigz(runtime_params): 
		"""
		Compresses the CSV file obtained from the previous task using the PigZ utility.

		Parameters:

		XCom Pushes:
			local_zip_path (str): The local file path to the compressed ZIP file.

		Raises:
			ValueError: If any errors occur while running the PigZ subprocess or if the ZIP file is not found.
		"""

		# Get the name of the CSV file at the end of the file path 
		csv_file_name = runtime_params.get("csv_file_name")
		
		# Change the current directory to where the CSV file is
		local_output_dir = runtime_params.get("local_output_dir")
		os.chdir(local_output_dir)
		
		# Run pigz to compress the CSV file in the current directory
		proc = subprocess.Popen( 
			f'pigz {csv_file_name}',
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE
		)

		# Log any errors when running PigZ subprocess
		out, err = proc.communicate()
		if err != b'':
			raise ValueError(f'The following error occured while running pigZ: {err}')
		
		# Check the zip file was created with the correct name
		zip_file_name = runtime_params.get("zip_file_name")
		zip_path = os.path.join(local_output_dir, zip_file_name)
		if os.path.exists(zip_path) is False:
			raise ValueError(f"Zip file does not exist: {zip_path}")
		
		logging.info(f"Successfully zipped CSV file with PigZ: {zip_path}")

		# return zip_path

	@task
	def load_zip_to_s3(runtime_params):
		"""
		Loads the ZIP file obtained from the previous task to the specified S3 bucket.

		Parameters:

		XCom Pulls:
			local_zip_path (str): The local file path to the compressed ZIP file.
		"""

		local_output_dir = runtime_params.get("local_output_dir")
		zip_file_name = runtime_params.get("zip_file_name")
		local_zip_path = os.path.join(local_output_dir, zip_file_name)

		# Load zip file to S3
		get_s3_hook().load_file(
			filename=local_zip_path,
			bucket_name=Variable.get("s3_etldata_bucket_var"),
			replace=True
		)

		# Delete the file and directory created locally
		os.remove(local_zip_path)
		os.rmdir(os.path.dirname(local_zip_path))


	@task
	def copy_s3_to_snowflake(runtime_params):
		"""
		Copies data from the S3 bucket to the Snowflake database table specified in the runtime parameters.

		The SQL query is constructed dynamically based on the data and runtime parameters.
		The data is copied using the "COPY INTO" Snowflake SQL command.

		Parameters:

		XCom Pulls:
			sf_table_name (str): The name of the target Snowflake table.
			columns (str): The columns to be copied.
			previous_day (str): The date used in the query.
			s3_file_path (str): The S3 bucket file path where the data is located.
			zip_file_name (str): The name of the ZIP file containing the data.
		"""
		sf_table_name = runtime_params.get("sf_table_name")
		columns = runtime_params.get("columns")
		previous_day = runtime_params.get("aod")
		s3_file_path = runtime_params.get("s3_file_path")
		zip_file_name = runtime_params.get("zip_file_name")

		sql_query = f"""
			COPY INTO STG.{sf_table_name} 
			FROM (
				SELECT 
					METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{previous_day}'), 
					{columns} 
				FROM @ETL.INBOUND/{s3_file_path}/
			)
			FILE_FORMAT = ( 
				FORMAT_NAME = {FILE_FORMAT}
			) 
			PATTERN = '.*{zip_file_name}.*'
		"""
		get_sf_hook().run(sql_query)


	# - - - - - - - - - - - - - - - - - - - - - - - - - - - 

	start = DummyOperator(task_id="start")
	end = DummyOperator(task_id="end", trigger_rule="all_done")

	trigger_DQ = TriggerDagRunOperator(
		task_id=f"trigger_DQ_{DATABASE_NAME}",
		trigger_dag_id=f"DQ_{DATABASE_NAME}",
		reset_dag_run=True,
		wait_for_completion=True,
		poke_interval=30,
		execution_date='{{ ds }}',
		# dag=Staging_CREO
	)
	
	for sf_table_name in fullTableList: 
		with TaskGroup(group_id=f"{sf_table_name}_Task") as export_mssql_load_snowflake:
			runtime_params = get_runtime_params(sf_table_name, ds="2023-07-01")

			reset_snowflake_task = reset_snowflake(runtime_params)
			
			run_bcp_task = run_bcp(runtime_params)
			
			run_pigz_task = run_pigz(runtime_params)
			load_zip_to_s3_task = load_zip_to_s3(runtime_params)
			
			copy_s3_to_snowflake_task = copy_s3_to_snowflake(runtime_params)

			reset_snowflake_task >> run_bcp_task >> run_pigz_task >> load_zip_to_s3_task >> copy_s3_to_snowflake_task

	start >> export_mssql_load_snowflake >> end >> trigger_DQ

staging_creo = Staging_CREO()
