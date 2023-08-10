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
	"CREO_CAMPAIGNTYPE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "CAMPAIGN_TYPE_KEY"
	},
	"CREO_COMMUNICATION_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "COMMUNICATION_KEY"
	},
	"CREO_COMMUNICATIONMAILING_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_START",
		"key_column": "COMMUNICATION_MAILING_KEY"
	},
	"CREO_CONFIG_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "CONFIG_KEY"
	},
	"CREO_CONFIGHISTORY_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "CONFIG_HISTORY_KEY"
	},
	"CREO_CONTACT_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "CONTACT_KEY"
	},
	"CREO_CONTACTTYPE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "CONTACT_TYPE_KEY"
	},
	"CREO_CONTAINER_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "CONTAINER_KEY"
	},
	"CREO_DATASET_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "DATASET_KEY"
	},
	"CREO_DATASETCELL_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_DATASETCOLUMN_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "DATASET_COLUMN_KEY"
	},
	"CREO_DATASETROW_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "DATASET_ROW_KEY"
	},
	"CREO_DATASETVALUE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "DATASET_VALUE_KEY"
	},
	"CREO_DATASOURCE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "DATASOURCE_KEY"
	},
	"CREO_DEADMESSAGES_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": None
	},
	"CREO_DEADMESSAGES2_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": None
	},
	"CREO_DELIVERYSTATUS_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "DELIVERY_STATUS_KEY"
	},
	"CREO_EMOJI_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "EMOJI_KEY"
	},
	"CREO_FOLDER_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "FOLDER_KEY"
	},
	"CREO_FOLDERCONTACT_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_FOLDERMESSAGE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_GLOBAL_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_LOG_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "LOG_KEY"
	},
	"CREO_MESSAGE_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "MESSAGE_KEY"
	},
	"CREO_MESSAGECONTACT_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "MESSAGE_CONTACT_KEY"
	},
	"CREO_MESSAGECONTACTTYPE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "MESSAGE_CONTACT_TYPE_KEY"
	},
	"CREO_MESSAGECONTACTV2_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_MESSAGEDELIVERYSTATUS_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "MESSAGE_DELIVERY_STATUS_KEY"
	},
	"CREO_MESSAGEPART_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "MESSAGE_PART_KEY"
	},
	"CREO_MESSAGEPARTV2_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_MESSAGESTATUSQUEUE_HIST": {
		"sql": None,
		"table_filtered_by": "ENTERED_AT",
		"key_column": "MESSAGE_STATUS_QUEUE_KEY"
	},
	"CREO_MESSAGETYPE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "MESSAGE_TYPE_KEY"
	},
	"CREO_PACKAGE_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "PACKAGE_KEY"
	},
	"CREO_PACKAGETEMPLATE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_PARAMETER_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "PARAMETER_KEY"
	},
	"CREO_RULE_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "RULE_KEY"
	},
	"CREO_TEMPLATE_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "TEMPLATE_KEY"
	},
	"CREO_TEMPLATERULE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": None
	},
	"CREO_TEMPLATETYPE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "TEMPLATE_TYPE_KEY"
	},
	"CREO_TEMPMESSAGE_HIST": {
		"sql": None,
		"table_filtered_by": None,
		"key_column": "TEMP_MESSAGE_KEY"
	},
	"CREO_USER_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": "USER_KEY"
	},
	"CREO_WEBHOOK_HIST": {
		"sql": None,
		"table_filtered_by": "DATE_ENTERED",
		"key_column": None
	}
}

# Constants >> Defined in upper case to distinguish from locally defined variables
DATABASE_NAME = "CREO"
FILE_FORMAT = "STG.LD_CSV_PIPE_SH1_EON_GZ"
SQL_DIR = f"/usr/local/airflow/include/sql/Staging_{DATABASE_NAME}"
START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')
TESTING = False

MSSQL_CONN = "awsmssql_creosql_creo_conn"
SF_CONN = "snowflake_default"

S3_CONN = "aws_s3_dev_conn"
S3_BUCKET = "s3-dev-bucket"
MSSQL_HOOK = MsSqlHook(MSSQL_CONN_id=MSSQL_CONN)
SF_HOOK = SnowflakeHook(SF_CONN_id=SF_CONN)
S3_HOOK = S3Hook(aws_conn_id=S3_CONN)
S3_BUCKET = Variable.get(S3_BUCKET)

## Default DAG args
default_args = {
	"retries": 0,
	"retry_delay": timedelta(minutes=1),
	'execution_timeout': timedelta(hours=3),
	"on_failure_callback": ms_teams_callback_functions.failure_callback
}

## Initialize the DAG
@dag(
	f"Staging_{DATABASE_NAME}",
	start_date=START_DATE,
	catchup=False,
	schedule_interval='0 6 * * *',  # 6AM EST
	dagrun_timeout=timedelta(hours=4),
	doc_md=__doc__,
	template_searchpath=f"include/sql/Staging_{DATABASE_NAME}/",
	default_args=default_args
)
def Staging_CREO():

	@task
	def get_runtime_params(sf_table_name, ds=None, **context) -> dict:
		"""
		Fetches runtime parameters and sets them in the context dictionary.

		Parameters:
			sf_table_name (str): The name of the Snowflake table to export data to.
			ds (str, optional): The date string in the format "YYYY-MM-DD". Defaults to None.
			**context (dict): The context dictionary containing the XCom variables.

		XCom Pushes:
			Runtime parameters:
				- sf_table_name (str): The name of the Snowflake table.
				- trunc_table_name (str): The truncated table name without database prefix and "_HIST".
				- columns (str): The columns to be copied.
				- asOfDate (datetime): The date as a datetime object.
				- s3_file_path (str): The S3 bucket file path where the data will be stored.
				- csv_file_name (str): The name of the CSV file to be created.
				- zip_file_name (str): The name of the ZIP file to be created.
		"""
		query_output = SF_HOOK.get_first(f"""
	      SELECT ETL.COPYSELECT('STG','{sf_table_name.upper()}',3)
		""")

		columns = query_output[f"ETL.COPYSELECT('STG','{sf_table_name.upper()}',3)"]
		aod = datetime.strptime(ds, "%Y-%m-%d")
		trunc_table_name = sf_table_name.replace(f"{DATABASE_NAME}_","").replace("_HIST","")
		s3_file_path = aod.strftime(f"{DATABASE_NAME}/%Y/%m/{trunc_table_name}/")
		csv_file_name = aod.strftime(f"{trunc_table_name}_%Y%m%d.csv.gz")
		zip_file_name = csv_file_name + '.gz'

		context["ti"].xcom_push(key="sf_table_name", value=sf_table_name)
		context["ti"].xcom_push(key="trunc_table_name", value=trunc_table_name)
		context["ti"].xcom_push(key="columns", value=columns)
		context["ti"].xcom_push(key="asOfDate", value=aod)
		context["ti"].xcom_push(key="s3_file_path", value=s3_file_path)
		context["ti"].xcom_push(key="csv_file_name", value=csv_file_name)
		context["ti"].xcom_push(key="zip_file_name", value=zip_file_name)


	# Supporting functions for manipulating and extracting data
	def reset_table_in_snowflake(sf_table_name, asOfDate, s3_path=None):
		"""
		Resets the data in the specified Snowflake table based on the provided parameters.

		Parameters:
			sf_table_name (str): The name of the Snowflake table.
			asOfDate (datetime): The date as a datetime object.
			s3_path (str, optional): The S3 file path. Defaults to None.
		"""

		if s3_path:
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE FILENAME = '{s3_path}';
			"""
		else:
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE date_entered = '{asOfDate}';
			"""
		
		SF_HOOK.run(sf_sql)

	def use_sql_file(asOfDate, sql_file):
		"""
		Reads and returns the SQL query from the specified SQL file.

		Parameters:
			asOfDate (datetime): The date as a datetime object.
			sql_file (str): The name of the SQL file to read the query from.

		Returns:
			str: The SQL query with the provided date formatted in the file.
		"""
		with open(f"{SQL_DIR}/{sql_file}", "r") as file:
			mssql_query = file.read().format(asOfDate)
			print(mssql_query)
		return mssql_query

	def use_date_column(asOfDate, trunc_table_name, table_filtered_by):
		"""
		Generates and returns the SQL query to select data based on the provided date column.

		Parameters:
			asOfDate (datetime): The date as a datetime object.
			trunc_table_name (str): The truncated table name without database prefix and "_HIST".
			table_filtered_by (str): The name of the date column to filter data.

		Returns:
			str: The SQL query to select data based on the provided date column and date.
		"""
		mssql_query = f"""
			SELECT * FROM dbo.{trunc_table_name} 
			WHERE CAST({table_filtered_by} AS DATE) = '{asOfDate}';
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
		max_value_df = SF_HOOK.get_pandas_df(snowflake_query)
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


	@task
	def get_mssql_query(**context):
		"""
		Retrieves the MSSQL query based on runtime parameters and data configurations.
		The query is determined based on various conditions, including the presence
		of an SQL file, a date column, or a key column.
		The resulting query is stored in the XCom variable "mssql_query" for later use.

		Parameters:
			**context (dict): The context dictionary containing the XCom variables.
		"""

		sf_table_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="sf_table_name")
		trunc_table_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="trunc_table_name")
		asOfDate = context["ti"].xcom_pull(task_ids="get_runtime_params", key="asOfDate")
		s3_file_path = context["ti"].xcom_pull(task_ids="get_runtime_params", key="s3_file_path")
		zip_file_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="zip_file_name")

		table_list = fullTableList.get(sf_table_name)
		if table_list:
			sql_file = table_list.get("sql")
			table_filtered_by = table_list.get("table_filtered_by")
			key_column = table_list.get("key_column", False)

		if sql_file:
			reset_table_in_snowflake(sf_table_name, asOfDate)
			query = use_sql_file(asOfDate, sql_file)

		elif table_filtered_by:
			reset_table_in_snowflake(sf_table_name, asOfDate)
			query = use_date_column(asOfDate, trunc_table_name, table_filtered_by)

		elif key_column:
			s3_path = s3_file_path + zip_file_name
			reset_table_in_snowflake(sf_table_name, asOfDate, s3_path)
			query = use_key_column(sf_table_name, trunc_table_name, key_column)

		else:
			reset_table_in_snowflake(sf_table_name, asOfDate)
			query = no_sql_date_keycol(trunc_table_name)

		context["ti"].xcom_push(key="mssql_query", value=query)


	# Supporting function for exporting MSSQL data with BCP
	def execute_bcp_utility_command(csv_out_path, query, delimiter='|', linebreak=os.linesep):
		"""
		Executes the BCP (Bulk Copy Program) utility command to export data from the MSSQL database.

		Parameters:
			csv_out_path (str): The file path to save the exported CSV file.
			query (str): The SQL query to execute for data export.
			delimiter (str, optional): The delimiter to use in the CSV file. Defaults to '|'.
			linebreak (str, optional): The linebreak character in the CSV file. Defaults to os.linesep.

		Raises:
			Exception: If the CSV file is not found after the export.
		"""

		# Connect to the MSSQL database using the Airflow connection
		conn = MSSQL_HOOK.get_conn()

		# Construct the BCP command
		bcp_cmd = [
			"/opt/mssql-tools/bin/bcp",
			f"{query}",
			"queryout", csv_out_path, 
			"-c",
			"-t" + delimiter,
			"-r" + linebreak,
			"-T", 
			"-S", MSSQL_HOOK.get_sqlalchemy_engine().url.host,  # Use the host from the connection
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

	@task
	def run_bcp(**context):
		"""
		Executes the BCP utility command to export data from the MSSQL database.

		Fetches the SQL query from the XCom variable "mssql_query" and writes the exported data
		to a CSV file.

		Parameters:
			**context (dict): The context dictionary containing the XCom variables.

		XCom Pushes:
			local_output_dir (str): The local directory path where the CSV file is exported.
		"""

		query = context["ti"].xcom_pull(task_ids="get_mssql_query", key="mssql_query")
		trunc_table_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="trunc_table_name")
		csv_file_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="csv_file_name")
		
		# Define and create the output directory where the CSV will be exported
		local_output_dir = datetime.now().strftime(f"include/bcp/Staging_{DATABASE_NAME}/%Y/%m/%d/{trunc_table_name}")
		os.makedirs(local_output_dir, exist_ok=True)
		context["ti"].xcom_push(key="local_output_dir", value=local_output_dir)

		csv_out_path = os.path.join(local_output_dir, csv_file_name)

		execute_bcp_utility_command(csv_out_path, query)


	@task
	def run_pigz(**context): 
		"""
		Compresses the CSV file obtained from the previous task using the PigZ utility.

		Parameters:
			**context (dict): The context dictionary containing the XCom variables.

		XCom Pushes:
			local_zip_path (str): The local file path to the compressed ZIP file.

		Raises:
			ValueError: If any errors occur while running the PigZ subprocess or if the ZIP file is not found.
		"""

		# Get the name of the CSV file at the end of the file path 
		csv_file_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="csv_file_name")
		
		# Change the current directory to where the CSV file is
		local_output_dir = context["ti"].xcom_pull(task_ids="run_bcp", key="local_output_dir")
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
		zip_file_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="zip_file_name")
		zip_path = os.path.join(local_output_dir, zip_file_name)
		if os.path.exists(zip_path) is False:
			raise ValueError(f"Zip file does not exist: {zip_path}")
		
		context["ti"].xcom_push(key="local_zip_path", value=zip_path)
		logging.info(f"Successfully zipped CSV file with PigZ: {zip_path}")


	@task
	def load_zip_to_s3(**context):
		"""
		Loads the ZIP file obtained from the previous task to the specified S3 bucket.

		Parameters:
			**context (dict): The context dictionary containing the XCom variables.

		XCom Pulls:
			local_zip_path (str): The local file path to the compressed ZIP file.
		"""

		local_zip_path = context["ti"].xcom_push(task_ids="run_pigz", key="local_zip_path")

		# Load zip file to S3
		S3_HOOK.load_file(
			filename=local_zip_path,
			bucket_name=S3_BUCKET,
			replace=True
		)

		# Delete the file and directory created locally
		os.remove(local_zip_path)
		os.rmdir(os.path.dirname(local_zip_path))


	@task
	def copy_s3_to_snowflake(**context):
		"""
		Copies data from the S3 bucket to the Snowflake database table specified in the runtime parameters.

		The SQL query is constructed dynamically based on the data and runtime parameters.
		The data is copied using the "COPY INTO" Snowflake SQL command.

		Parameters:
			**context (dict): The context dictionary containing the XCom variables.

		XCom Pulls:
			sf_table_name (str): The name of the target Snowflake table.
			columns (str): The columns to be copied.
			previous_day (str): The date used in the query.
			s3_file_path (str): The S3 bucket file path where the data is located.
			zip_file_name (str): The name of the ZIP file containing the data.
		"""

		sf_table_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="sf_table_name")
		columns = context["ti"].xcom_pull(task_ids="get_runtime_params", key="columns")
		previous_day = context["ti"].xcom_pull(task_ids="get_runtime_params", key="asOfDate")
		s3_file_path = context["ti"].xcom_pull(task_ids="get_runtime_params", key="s3_file_path")
		zip_file_name = context["ti"].xcom_pull(task_ids="get_runtime_params", key="zip_file_name")

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
		SF_HOOK.run(sql_query)


	# - - - - - - - - - - - - - - - - - - - - - - - - - - - 

	start = DummyOperator(task_id="start", dag=staging_creo)
	end = DummyOperator(task_id="end", trigger_rule="all_done", dag=staging_creo)

	trigger_DQ = TriggerDagRunOperator(
		task_id=f"trigger_DQ_{DATABASE_NAME}",
		trigger_dag_id=f"DQ_{DATABASE_NAME}",
		reset_dag_run=True,
		wait_for_completion=True,
		poke_interval=30,
		execution_date='{{ ds }}',
		dag=staging_creo
	)
	
	for sf_table_name in fullTableList: 
		with TaskGroup(group_id=f"mssql_to_s3_{sf_table_name}") as export_mssql_load_snowflake:
			get_runtime_params = PythonOperator(
				task_id=f'get_runtime_params_{DATABASE_NAME}_{sf_table_name}',
				python_callable=get_runtime_params,
				op_args=[sf_table_name],
			)
			get_mssql_query = PythonOperator(
				task_id=f'get_mssql_query_{DATABASE_NAME}_{sf_table_name}',
				python_callable=get_mssql_query,
			)
			run_bcp = PythonOperator(
				task_id=f'run_bcp_{DATABASE_NAME}_{sf_table_name}',
				python_callable=run_bcp,
			)
			run_pigz = PythonOperator(
				task_id=f'run_pigz_{DATABASE_NAME}_{sf_table_name}',
				python_callable=run_pigz,
			)
			load_zip_to_s3 = PythonOperator(
				task_id=f'load_zip_to_s3_{DATABASE_NAME}_{sf_table_name}',
				python_callable=load_zip_to_s3,
			)
			copy_s3_to_snowflake = PythonOperator(
				task_id=f"copy_to_sf_{sf_table_name}",
				python_callable=copy_s3_to_snowflake,
			)
			get_runtime_params >> get_mssql_query >> run_bcp >> run_pigz >> load_zip_to_s3 >> copy_s3_to_snowflake

	start >> export_mssql_load_snowflake >> end >> trigger_DQ

staging_creo = Staging_CREO()
