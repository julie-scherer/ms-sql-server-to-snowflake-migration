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
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import pendulum
import json

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
		"key_column": "APPROVAL_REQUEST_KEY",
		"keys": "{'APPROVAL_REQUEST_KEY', 'PACKAGE_KEY'}",
		"table_filtered_by": "ENTERED_AT"
	},
	"CREO_APPROVALREQUESTITEM_HIST": {
		"sql": None,
		"key_column": "APPROVAL_REQUEST_ITEM_KEY",
		"keys": "{'APPROVAL_REQUEST_KEY', 'APPROVAL_REQUEST_ITEM_KEY', 'TEMPLATE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_CAMPAIGN_HIST": {
		"sql": None,
		"key_column": "CAMPAIGN_KEY",
		"keys": "{'CAMPAIGN_TYPE_KEY', 'CONTAINER_KEY', 'SEED_LIST_DATASET_KEY', 'CAMPAIGN_KEY', 'DATASET_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_CAMPAIGNTYPE_HIST": {
		"sql": None,
		"key_column": "CAMPAIGN_TYPE_KEY",
		"keys": "{'CAMPAIGN_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_COMMUNICATION_HIST": {
		"sql": None,
		"key_column": "COMMUNICATION_KEY",
		"keys": "{'CAMPAIGN_KEY', 'COMMUNICATION_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_COMMUNICATIONMAILING_HIST": {
		"sql": None,
		"key_column": "COMMUNICATION_MAILING_KEY",
		"keys": "{'COMMUNICATION_KEY', 'COMMUNICATION_MAILING_KEY'}",
		"table_filtered_by": "DATE_COMPLETED"
	},
	"CREO_CONFIG_HIST": {
		"sql": None,
		"key_column": "CONFIG_KEY",
		"keys": "{'CONFIG_KEY'}",
		"table_filtered_by": None
	},
	"CREO_CONFIGHISTORY_HIST": {
		"sql": None,
		"key_column": "CONFIG_HISTORY_KEY",
		"keys": "{'CONFIG_KEY', 'CONFIG_HISTORY_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_CONTACT_HIST": {
		"sql": None,
		"key_column": "CONTACT_KEY",
		"keys": "{'CONTACT_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_CONTACTTYPE_HIST": {
		"sql": None,
		"key_column": "CONTACT_TYPE_KEY",
		"keys": "{'CONTACT_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_CONTAINER_HIST": {
		"sql": None,
		"key_column": "CONTAINER_KEY",
		"keys": "{'CONTAINER_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DATASET_HIST": {
		"sql": None,
		"key_column": "DATASET_KEY",
		"keys": "{'CONTAINER_KEY', 'DATASET_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DATASETCELL_HIST": {
		"sql": None,
		"key_column": "DATASET_ROW_KEY",
		"keys": "{'DATASET_VALUE_KEY', 'DATASET_COLUMN_KEY', 'DATASET_ROW_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASETCOLUMN_HIST": {
		"sql": None,
		"key_column": "DATASET_COLUMN_KEY",
		"keys": "{'DATASET_COLUMN_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASETROW_HIST": {
		"sql": None,
		"key_column": "DATASET_ROW_KEY",
		"keys": "{'DATASET_KEY', 'DATASET_ROW_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASETVALUE_HIST": {
		"sql": None,
		"key_column": "DATASET_VALUE_KEY",
		"keys": "{'DATASET_VALUE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASOURCE_HIST": {
		"sql": None,
		"key_column": "DATASOURCE_KEY",
		"keys": "{'DATASOURCE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DEADMESSAGES_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'DELIVERY_STATUS_KEY', 'CONTAINER_KEY', 'SEND_AFTER_MESSAGE_KEY', 'COMMUNICATION_MAILING_KEY', 'IN_REPLY_TO_MESSAGE_KEY', 'TEMPLATE_KEY', 'DATASET_ROW_KEY', 'MESSAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DEADMESSAGES2_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'DELIVERY_STATUS_KEY', 'CONTAINER_KEY', 'SEND_AFTER_MESSAGE_KEY', 'COMMUNICATION_MAILING_KEY', 'IN_REPLY_TO_MESSAGE_KEY', 'TEMPLATE_KEY', 'DATASET_ROW_KEY', 'MESSAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DELIVERYSTATUS_HIST": {
		"sql": None,
		"key_column": "DELIVERY_STATUS_KEY",
		"keys": "{'DELIVERY_STATUS_KEY'}",
		"table_filtered_by": None
	},
	"CREO_EMOJI_HIST": {
		"sql": None,
		"key_column": "EMOJI_KEY",
		"keys": "{'EMOJI_KEY'}",
		"table_filtered_by": None
	},
	"CREO_FOLDER_HIST": {
		"sql": None,
		"key_column": "FOLDER_KEY",
		"keys": "{'FOLDER_KEY', 'CONTAINER_KEY', 'PARENT_FOLDER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_FOLDERCONTACT_HIST": {
		"sql": None,
		"key_column": "CONTACT_KEY",
		"keys": "{'CONTACT_KEY', 'FOLDER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_FOLDERMESSAGE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'MESSAGE_KEY', 'FOLDER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_GLOBAL_HIST": {
		"sql": None,
		"key_column": "APP_VERSION",
		"keys": "set()",
		"table_filtered_by": None
	},
	"CREO_LOG_HIST": {
		"sql": None,
		"key_column": "LOG_KEY",
		"keys": "{'CONTAINER_KEY', 'RULE_KEY', 'CAMPAIGN_KEY', 'TEMPLATE_KEY', 'PACKAGE_KEY', 'COMMUNICATION_KEY', 'MESSAGE_KEY', 'LOG_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_MESSAGE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'DELIVERY_STATUS_KEY', 'CONTAINER_KEY', 'SEND_AFTER_MESSAGE_KEY', 'COMMUNICATION_MAILING_KEY', 'IN_REPLY_TO_MESSAGE_KEY', 'TEMPLATE_KEY', 'DATASET_ROW_KEY', 'MESSAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_MESSAGECONTACT_HIST": {
		"sql": None,
		"key_column": "MESSAGE_CONTACT_KEY",
		"keys": "{'MESSAGE_CONTACT_KEY', 'MESSAGE_KEY', 'CONTACT_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGECONTACTTYPE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_CONTACT_TYPE_KEY",
		"keys": "{'MESSAGE_CONTACT_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGECONTACTV2_HIST": {
		"sql": None,
		"key_column": "MESSAGE_CONTACT_KEY",
		"keys": "{'MESSAGE_CONTACT_KEY', 'MESSAGE_KEY', 'CONTACT_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGEDELIVERYSTATUS_HIST": {
		"sql": None,
		"key_column": "MESSAGE_DELIVERY_STATUS_KEY",
		"keys": "{'MESSAGE_KEY', 'MESSAGE_DELIVERY_STATUS_KEY', 'DELIVERY_STATUS_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_MESSAGEPART_HIST": {
		"sql": None,
		"key_column": "MESSAGE_PART_KEY",
		"keys": "{'MESSAGE_KEY', 'MESSAGE_PART_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGEPARTV2_HIST": {
		"sql": None,
		"key_column": "MESSAGE_PART_KEY",
		"keys": "{'MESSAGE_KEY', 'MESSAGE_PART_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGESTATUSQUEUE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_STATUS_QUEUE_KEY",
		"keys": "{'CONTAINER_KEY', 'MESSAGE_STATUS_QUEUE_KEY'}",
		"table_filtered_by": "ENTERED_AT"
	},
	"CREO_MESSAGETYPE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_TYPE_KEY",
		"keys": "{'MESSAGE_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_PACKAGE_HIST": {
		"sql": None,
		"key_column": "PACKAGE_KEY",
		"keys": "{'CONTAINER_KEY', 'PACKAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_PACKAGETEMPLATE_HIST": {
		"sql": None,
		"key_column": "PACKAGE_KEY",
		"keys": "{'TEMPLATE_KEY', 'PACKAGE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_PARAMETER_HIST": {
		"sql": None,
		"key_column": "PARAMETER_KEY",
		"keys": "{'DATASOURCE_KEY', 'PARAMETER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_RULE_HIST": {
		"sql": None,
		"key_column": "RULE_KEY",
		"keys": "{'RULE_KEY', 'CONTAINER_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_TEMPLATE_HIST": {
		"sql": None,
		"key_column": "TEMPLATE_KEY",
		"keys": "{'TEMPLATE_KEY', 'BASE_TEMPLATE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_TEMPLATERULE_HIST": {
		"sql": None,
		"key_column": "TEMPLATE_KEY",
		"keys": "{'RULE_KEY', 'TEMPLATE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_TEMPLATETYPE_HIST": {
		"sql": None,
		"key_column": "TEMPLATE_TYPE_KEY",
		"keys": "{'TEMPLATE_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_TEMPMESSAGE_HIST": {
		"sql": None,
		"key_column": "TEMP_MESSAGE_KEY",
		"keys": "{'TEMP_MESSAGE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_USER_HIST": {
		"sql": None,
		"key_column": "USER_KEY",
		"keys": "{'USER_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_WEBHOOK_HIST": {
		"sql": None,
		"key_column": "WEBHOOK_KEY",
		"keys": "{'CONTAINER_KEY', 'WEBHOOK_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	}
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

AS_OF_DATE = "2023-06-01"


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



def get_table_list():
	dfTableList = get_sf_hook().get_pandas_df(f"""SELECT * FROM ARES.ETL.{DATABASE_NAME}_UTILS""")
	logging.info(f"dfTableList = {dfTableList}, {type(dfTableList)}")
	table_list = dfTableList["TABLE_NAME"].to_list()
	logging.info(f"table_list = {table_list}, {type(table_list)}")
	return table_list



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
	logging.info(f"max value df = {max_value_df}")

	max_value = 0
	if max_value_df.size != 0 and max_value_df.iloc[0, 0] != None:
		max_value = max_value_df.iloc[0, 0]
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
		
		runtime_params = {}
		trunc_table_name = sf_table_name.replace(f"{DATABASE_NAME}_","").replace("_HIST","")
		get_numbered_cols = get_sf_hook().get_first(f"""SELECT ETL.COPYSELECT('STG','{sf_table_name.upper()}',3)""")
		columns = get_numbered_cols[f"ETL.COPYSELECT('STG','{sf_table_name.upper()}',3)"]
		aod = datetime.strptime(ds, "%Y-%m-%d")
		s3_file_path = aod.strftime(f"{DATABASE_NAME}/%Y/%m/{trunc_table_name}/")
		csv_file_name = aod.strftime(f"{trunc_table_name}_%Y%m%d.csv")
		zip_file_name = csv_file_name + '.gz'
		local_output_dir = datetime.now().strftime(f"include/bcp/Staging_{DATABASE_NAME}/%Y/%m/%d/{trunc_table_name}")

		table_utils = get_sf_hook().get_first(f"""SELECT * FROM ARES.ETL.{DATABASE_NAME}_UTILS WHERE table_name = '{sf_table_name}'""")
		logging.info(f"get_creo_utils = {table_utils}, {type(table_utils)}")

		runtime_params = {
			"sf_table_name": sf_table_name,
			"trunc_table_name": trunc_table_name,
			"columns": columns,
			"aod": aod,
			"s3_file_path": s3_file_path,
			"csv_file_name": csv_file_name,
			"zip_file_name": zip_file_name,
			"local_output_dir": local_output_dir,
			"table_utils": table_utils,
			"sql": table_utils.get("SQL"),
			"key_column": table_utils.get("KEY_COLUMN"),
			"keys": table_utils.get("KEYS"),
			"table_filtered_by": table_utils.get("TABLE_FILTERED_BY"),
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

		sql_file = runtime_params.get("sql")
		table_filtered_by = runtime_params.get("table_filtered_by")
		key_column = runtime_params.get("key_column")

		if sql_file != 'NULL':
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE CAST(DATE_ENTERED AS DATE) = TO_DATE('{aod}');
			"""

		elif table_filtered_by != 'NULL':
			sf_sql = f"""
				DELETE FROM STG.{sf_table_name} 
				WHERE ASOFDATE = TO_DATE('{aod}');
			"""

		elif key_column != 'NULL':
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
		return sf_sql


	@task
	def mssql_to_s3(runtime_params): 
		"""
		Load MSSQL data from Pandas DataFrame to S3.

		df, s3_file_path, s3_csv_file
		"""
		trunc_table_name = runtime_params.get("trunc_table_name")
		aod = runtime_params.get("aod")
		s3_file_path = runtime_params.get("s3_file_path")
		zip_file_name = runtime_params.get("zip_file_name")

		sql_file = runtime_params.get("sql")
		table_filtered_by = runtime_params.get("table_filtered_by")
		key_column = runtime_params.get("key_column")

		if sql_file != 'NULL':
			mssql_query = use_sql_file(aod, sql_file)

		elif table_filtered_by != 'NULL':
			mssql_query = use_date_column(aod, trunc_table_name, table_filtered_by)

		elif key_column != 'NULL':
			mssql_query = use_key_column(sf_table_name, trunc_table_name, key_column)

		else:
			mssql_query = no_sql_date_keycol(trunc_table_name)


		df = get_mssql_hook().get_pandas_df(sql=mssql_query)
		df_byte = df \
			.replace({np.nan: 'NULL'}) \
			.to_csv(
				# header=False,  # Exclude the column headers from the CSV
				index=False,  # Exclude the row index from the CSV
				sep="|",  # Use the pipe symbol as the column separator
				# na_rep='NULL',  # Replace missing values with 'NULL'
				# compression='gzip',  # Compress the CSV file using gzip
				doublequote=True,  # Enable double quoting for values
				quotechar='"'
			).encode()

		get_s3_hook().load_bytes(
			bytes_data=df_byte,
			bucket_name=Variable.get("s3_etldata_bucket_var"),
			replace=True,
			key=f"{s3_file_path}/{zip_file_name}"
		)


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

	# fullTableList = get_table_list()
	for sf_table_name in fullTableList: 
		logging.info(f"Running {sf_table_name}")
		with TaskGroup(group_id=f"{sf_table_name}_Task") as export_mssql_load_snowflake:
			runtime_params = get_runtime_params(sf_table_name, ds=AS_OF_DATE)

			reset_snowflake_task = reset_snowflake(runtime_params)
			mssql_to_s3_task = mssql_to_s3(runtime_params)
			copy_s3_to_snowflake_task = copy_s3_to_snowflake(runtime_params)

			runtime_params >> reset_snowflake_task >> mssql_to_s3_task >> copy_s3_to_snowflake_task

	start >> export_mssql_load_snowflake >> end >> trigger_DQ

staging_creo = Staging_CREO()
