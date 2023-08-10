"""
### Load CREO data to Snowflake 
Scheduled to run every morning at 6AM EST
This DAG copies data for the below listed tables based on table type (increamental, full-load) to Snowflake.

BRANCH
	-  AB#203037-CREO-Staging-DAG

KEY FEATURES
	- SQL driven 
		- move full table lists from Staging DAGs to ARES.ETL.Utils tables for CREO, CM, and LD
		- get fields from ETL.UTILS table instead of from fullTableList dictionary
	- Exporting data using BCP
	- Compressing data using PigZ
	...
	++ Load to S3 & copy into Snowflake

DOCS/REFERENCES
	- https://github.com/CuroFinTechCorp/ETL_and_Database/blob/release/airflow/dags/functions/etlutils.py
	- https://docs.astronomer.io/learn/airflow-snowflake
	- https://registry.astronomer.io/providers/snowflake/versions/latest/modules/snowflakehook
"""

import os
import logging
from pathlib import Path
import subprocess
from datetime import datetime, timedelta
import pendulum
import numpy as np
import pandas as pd

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils import trigger_rule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from include.functions import ms_teams_callback_functions
import include.sql.staging_sql_statements as sql_stmts


fullTableList = { #! WILL BE DEPRECIATED -- `fullTableList` WILL BE REPLACED WITH ETL.UTILS TABLE IN SF
	"CREO_APPROVALREQUEST_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "APPROVAL_REQUEST_KEY"
	},
	"CREO_APPROVALREQUESTITEM_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "APPROVAL_REQUEST_ITEM_KEY"
	},
	"CREO_CAMPAIGN_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "CAMPAIGN_KEY"
	},
	"CREO_CAMPAIGNTYPE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "CAMPAIGN_TYPE_KEY"
	},
	"CREO_COMMUNICATION_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "COMMUNICATION_KEY"
	},
	"CREO_COMMUNICATIONMAILING_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "COMMUNICATION_MAILING_KEY"
	},
	"CREO_CONFIG_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "CONFIG_KEY"
	},
	"CREO_CONFIGHISTORY_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "CONFIG_HISTORY_KEY"
	},
	"CREO_CONTACT_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "CONTACT_KEY"
	},
	"CREO_CONTACTTYPE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "CONTACT_TYPE_KEY"
	},
	"CREO_CONTAINER_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "CONTAINER_KEY"
	},
	"CREO_DATASET_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "DATASET_KEY"
	},
	"CREO_DATASETCELL_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_DATASETCOLUMN_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "DATASET_COLUMN_KEY"
	},
	"CREO_DATASETROW_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "DATASET_ROW_KEY"
	},
	"CREO_DATASETVALUE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "DATASET_VALUE_KEY"
	},
	"CREO_DATASOURCE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "DATASOURCE_KEY"
	},
	"CREO_DEADMESSAGES_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": None
	},
	"CREO_DEADMESSAGES2_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": None
	},
	"CREO_DELIVERYSTATUS_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "DELIVERY_STATUS_KEY"
	},
	"CREO_EMOJI_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "EMOJI_KEY"
	},
	"CREO_FOLDER_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "FOLDER_KEY"
	},
	"CREO_FOLDERCONTACT_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_FOLDERMESSAGE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_GLOBAL_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_LOG_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "LOG_KEY"
	},
	"CREO_MESSAGE_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "MESSAGE_KEY"
	},
	"CREO_MESSAGECONTACT_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "MESSAGE_CONTACT_KEY"
	},
	"CREO_MESSAGECONTACTTYPE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "MESSAGE_CONTACT_TYPE_KEY"
	},
	"CREO_MESSAGECONTACTV2_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_MESSAGEDELIVERYSTATUS_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "MESSAGE_DELIVERY_STATUS_KEY"
	},
	"CREO_MESSAGEPART_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "MESSAGE_PART_KEY"
	},
	"CREO_MESSAGEPARTV2_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_MESSAGESTATUSQUEUE_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "MESSAGE_STATUS_QUEUE_KEY"
	},
	"CREO_MESSAGETYPE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "MESSAGE_TYPE_KEY"
	},
	"CREO_PACKAGE_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "PACKAGE_KEY"
	},
	"CREO_PACKAGETEMPLATE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_PARAMETER_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "PARAMETER_KEY"
	},
	"CREO_RULE_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "RULE_KEY"
	},
	"CREO_TEMPLATE_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "TEMPLATE_KEY"
	},
	"CREO_TEMPLATERULE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": None
	},
	"CREO_TEMPLATETYPE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "TEMPLATE_TYPE_KEY"
	},
	"CREO_TEMPMESSAGE_HIST": {
		"sql": None,
		"has_date_entered": False,
		"key_column": "TEMP_MESSAGE_KEY"
	},
	"CREO_USER_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": "USER_KEY"
	},
	"CREO_WEBHOOK_HIST": {
		"sql": None,
		"has_date_entered": True,
		"key_column": None
	}
}

# Constants >> Defined in upper case to distinguish from locally defined variables
DATABASE_NAME = 'CREO'
FILE_FORMAT = 'CSV_PIPE_SH0_EON_GZ'
SQL_DIR = f"/usr/local/airflow/include/sql/Staging_{DATABASE_NAME}"
START_DATE = pendulum.datetime(2023, 2, 28, tz='US/Eastern')
TESTING = False

MSSQL_CONN = "awsmssql_creosql_creo_conn"
SF_CONN = "snowflake_default"

S3_CONN = "aws_s3_dev_conn"
S3_BUCKET = "s3-dev-bucket"
MSSQL_HOOK = MsSqlHook(MSSQL_CONN_id=MSSQL_CONN)
SF_HOOK = SnowflakeHook(SF_CONN_id=SF_CONN)
S3_HOOK = S3Hook(aws_conn_id=S3_CONN)
S3_BUCKET = Variable.get(S3_BUCKET)

## Logging function
def log(**kwargs):
	out_dir = make_dir(os.path.join(logs_dir, 'runs'))
	log_file_name = datetime.now().strftime(f'Run_%Y%m%d_%H%M.txt')
	log_file_path = os.path.join(out_dir, log_file_name)
	logging.basicConfig(
		filename=log_file_path,
		format='[%(levelname)s] %(asctime)s:  %(message)s',
		filemode='w'
	)
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	print(f"Log file saved at {log_file_path}")


## Default DAG args
default_args = {
	"retries": 0,
	"retry_delay": timedelta(minutes=1),
	'execution_timeout': timedelta(hours=3),
	"on_failure_callback": ms_teams_callback_functions.failure_callback
}

# Initialize the DAG
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
	## STEP 1 : Get runtime parameters

	def get_runtime_params(table_name, ds=None) -> dict:
		"""Get runtime parameters for the table ETL process."""
		output = SF_HOOK.get_first(f"SELECT ETL.COPYSELECT('STG','{table_name.upper()}',3)")
		columns = output[f"ETL.COPYSELECT('STG','{table_name.upper()}',3)"] #// getting the columns $ list for each table
		table_t = table_name.replace(f"{DATABASE_NAME}_","")[:-5] #// truncated table name with "{DATABASE_NAME}_" prefix and "_HIST" suffix removed
		aod = datetime.strptime(ds, "%Y-%m-%d") #// datestamp input in YYYY-MM-DD format
		year, month, date = aod.strftime("%Y"), aod.strftime("%m"), aod.strftime('%Y%m%d') #// extracting year, month, date in string format from asOfDate
		s3_file_path = f"{DATABASE_NAME}/{year}/{month}/{table_t}/" #// S3 file path
		s3_csv_file = f"{table_t}_{date}.csv.gz" #// name of CSV file that'll be stored in S3
		
		# * Returning runtime parameters to be used by load_mssql_df_to_s3() and copy_s3_to_snowflake() tasks
		return {
			"table": table_name,
			"columns": columns,
			"asOfDate": aod,
			"s3_file_path": s3_file_path,
			"s3_csv_file": s3_csv_file
		}

	def get_runtime_utils(DATABASE_NAME, table_name, field):
		"""IN DEVELOPMENT: Get runtime parameters from ETL.Utils in Snowflake (to replace get_runtime_params() function)."""
		return SnowflakeOperator(
			task_id="get_runtime_utils",
			sql=sql_stmts.get_runtime_utils,
			params={
				"table_name": table_name,
				"DATABASE_NAME": DATABASE_NAME,
				"field": field,
			},
		)

	## STEP 2 : Export MSSQL data 

	# >> Option 1, use Pandas -- will be depreciated

	def get_mssql_data_as_df(MSSQL_HOOK, mssql_query):
		"""Supporting function: Transform MSSQL output to Pandas DataFrame."""
		return MSSQL_HOOK.get_pandas_df(sql=mssql_query)

	def reset_table_in_snowflake(SF_HOOK, table, asOfDate, filename_path=None):
		"""Supporting function: Reset table in Snowflake."""
		if filename_path:
			sf_sql = f"""
				DELETE FROM STG.{table} 
				WHERE FILENAME = '{filename_path}';
			"""
		else:
			sf_sql = f"""
				DELETE FROM STG.{table} 
				WHERE date_entered = '{asOfDate}';
			"""
		SF_HOOK.run(sf_sql)

	def export_mssql_data(runtime_params: dict, table_name: str):
		"""Main export function that exports data from MSSQL to S3."""
		table = runtime_params.get("table")
		asOfDate = runtime_params.get("asOfDate")
		s3_file_path = runtime_params.get("s3_file_path")
		s3_csv_file = runtime_params.get("s3_csv_file")
		table_t = table.replace(f"{DATABASE_NAME}_", "")[:-5]

		table_list_name = fullTableList.get(table_name)
		# table_list_name = get_utils_query(DATABASE_NAME, table_name, 'table_name')
		if table_list_name:
			sql_file = table_list_name.get("sql") # sql_file = get_utils_query(DATABASE_NAME, table_name, 'sql')
			has_date_entered = table_list_name.get("has_date_entered") # has_date_entered = get_utils_query(DATABASE_NAME, table_name, 'has_date_entered')
			key_column = table_list_name.get("key_column", False) # key_column = get_utils_query(DATABASE_NAME, table_name, 'key_column')

		df = None
		if sql_file:
			df = use_sql_file(SF_HOOK, MSSQL_HOOK, table, asOfDate, SQL_DIR, sql_file)

		elif has_date_entered:
			df = use_date_column(SF_HOOK, MSSQL_HOOK, table, asOfDate, table_t)

		elif key_column:
			df = use_key_column(SF_HOOK, MSSQL_HOOK, table, asOfDate, table_name, table_t, key_column, S3_HOOK, S3_BUCKET, s3_file_path, s3_csv_file)

		else:
			df = no_sql_date_keycol(SF_HOOK, MSSQL_HOOK, table, asOfDate, table_t, S3_HOOK, S3_BUCKET, s3_file_path, s3_csv_file)

		print(df.columns)
		return df

	def use_sql_file(SF_HOOK, MSSQL_HOOK, table, asOfDate, SQL_DIR, sql_file):
		"""Use SQL file to export data from MSSQL."""
		reset_table_in_snowflake(SF_HOOK, table, asOfDate)

		with open(f"{SQL_DIR}/{sql_file}", "r") as file:
			dated_mssql_query = file.read().format(asOfDate)
			print(dated_mssql_query)
		
		df = get_mssql_data_as_df(MSSQL_HOOK, dated_mssql_query)
		return df

	def use_date_column(SF_HOOK, MSSQL_HOOK, table, asOfDate, table_t):
		"""Use DATE column to export data from MSSQL."""
		reset_table_in_snowflake(SF_HOOK, table, asOfDate)

		mssql_query = f"""
			SELECT * FROM dbo.{table_t} 
			WHERE CAST(DATE_ENTERED AS DATE) = '{asOfDate}';
		"""
		df = get_mssql_data_as_df(MSSQL_HOOK, mssql_query)
		return df

	def use_key_column(SF_HOOK, MSSQL_HOOK, table, asOfDate, table_name, table_t, key_column, s3_file_path, s3_csv_file):
		"""Use KEY COLUMN to export data from MSSQL."""
		filename_path = s3_file_path + s3_csv_file
		reset_table_in_snowflake(SF_HOOK, table, asOfDate, filename_path)

		snowflake_query = f"""
			SELECT MAX({key_column})
			FROM STG.{table_name}
		"""
		max_value_df = SF_HOOK.get_pandas_df(snowflake_query)
		max_value = max_value_df.iloc[0, 0]

		if max_value is None:
			max_value = 0
		print(max_value)

		mssql_query = f"""
			SELECT * 
			FROM dbo.{table_t} 
			WHERE {key_column} > {max_value};
		"""
		df = get_mssql_data_as_df(MSSQL_HOOK, mssql_query)
		return df

	def no_sql_date_keycol(SF_HOOK, MSSQL_HOOK, table, asOfDate, table_t):
		"""Export data from MSSQL in chunks if there's no custom SQL file, DATE column, nor KEY column."""
		reset_table_in_snowflake(SF_HOOK, table, asOfDate)
		dfs = [chunk for chunk in MSSQL_HOOK.get_pandas_df_by_chunks(sql=f"SELECT * FROM dbo.{table_t};", chunksize=1000)]
		df = pd.concat(dfs)
		return df

	# >> Option 2, use BCP and PigZ 

	def export_csv_using_bcp(DATABASE_NAME, table_name, csv_out_path, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
		"""IN DEVELOPMENT: Export MSSQL data using BCP utility."""
		logging.info(f"Running BCP command for {DATABASE_NAME}.dbo.{table_name}... ")
		try:
			Path(csv_out_path).touch(exist_ok=True)

			# Logs for testing
			# >> create folders
			bcp_logs = make_dir(os.path.join(logs_dir, 'bcp_run'))
			err_logs = make_dir(os.path.join(logs_dir, 'bcp_errs'))
			# >> set file path
			bcp_path = os.path.join(bcp_logs, datetime.now().strftime(f'BCP_{DATABASE_NAME}_{table_name}_%Y%m%d_%H%M_Log.txt'))
			err_path = os.path.join(err_logs, datetime.now().strftime(f'BCP_{DATABASE_NAME}_{table_name}_%Y%m%d_%H%M_Error.txt'))
			# >> create files if they don't exist
			Path(bcp_path).touch(exist_ok=True)
			Path(err_path).touch(exist_ok=True) 

			# Connect to the MSSQL database using the Airflow connection
			conn = MSSQL_HOOK.get_conn()

			# Construct the BCP command
			bcp_cmd = [
				"/opt/mssql-tools/bin/bcp",
				f"SELECT * FROM {DATABASE_NAME}.dbo.{table_name}",
				"queryout", csv_out_path, "-c",
				"-t" + delimiter, "-r" + linebreak,
				# "-T", "-S", server,
				"-T", "-S", MSSQL_HOOK.get_sqlalchemy_engine().url.host,  # Use the host from the connection
				"-d", f'{DATABASE_NAME}',
				"-o", bcp_path, #! logs cmd line output in txt file for testing
				"-e", err_path, #! saves errors in txt file for testing
			]

			# Execute the BCP command inside the MSSQL connection
			cursor = conn.cursor()
			cursor.execute(" ".join(bcp_cmd))
			cursor.close()
			conn.close()

			if not Path.is_file(csv_out_path):
				raise Exception(f'File not found: {csv_out_path}')
			
			logging.info(f"Successfully executed BCP for {DATABASE_NAME}.dbo.{table_name}: {csv_out_path}")
			logging.info(f">> File Size: {os.path.getsize(csv_out_path)} bytes")
			logging.info(f">> Path: {csv_out_path}")
			return True
		
		except Exception as e:
			logging.error(f"Error occurred while exporting {DATABASE_NAME}.dbo.{table_name}: {e}")
			return False

	def zip_bcp_csv_using_pigz(csv_source_path): 
		"""IN DEVELOPMENT: Compress CSV using PigZ to export MSSQL data."""
		logging.info(f"Running PigZ command for {csv_source_path}... ")
		try:
			zip_out_path = csv_source_path + '.gz'
			proc = subprocess.Popen(
				f'pigz {csv_source_path}'.split(maxsplit=1),
				stdout=subprocess.PIPE,
				stderr=subprocess.PIPE
			)

			out, err = proc.communicate()
			if err != b'':
				raise ValueError(f'The following error occured {err}')
			
			if not Path.is_file(zip_out_path):
				raise ValueError(f"File does not exist: {zip_out_path}")
			
			logging.info(f"Successfully executed PigZ: {zip_out_path}")
			return True
		
		except Exception as e:
			logging.error(f"Error occurred while compressing {csv_source_path}: {e}")
			return False

	## STEP 3 : Load MSSQL data to S3

	def load_mssql_df_to_s3(df, s3_file_path, s3_csv_file): 
		"""MSSQL PANDAS DF WILL BE DEPRECATED. Load MSSQL data from Pandas DataFrame to S3."""

		df_byte = df \
			.replace({np.nan: 'NULL'}) \
			.to_csv(
				compression="gzip", sep="|",
				index=False,
				quotechar='"'
			).encode()

		S3_HOOK.load_bytes(
			bytes_data=df_byte,
			bucket_name=S3_BUCKET,
			replace=True,
			key=f"inbound/{s3_file_path}{s3_csv_file}"
		)

		# return runtime_params

	def load_pigz_csv_to_s3(TBD):
		"""IN DEVELOPMENT, Load compressed CSV file to S3 bucket."""
		pass

	## STEP 4 : Run COPY INTO query in Snowflake

	def copy_s3_to_snowflake(runtime_params: dict):	
		"""Copy staged data from S3 to Snowflake."""
		table_name = runtime_params.get("table")
		columns = runtime_params.get("columns")
		previous_day = runtime_params.get("asOfDate")
		s3_file_path = runtime_params.get("s3_file_path")
		s3_csv_file = runtime_params.get("s3_csv_file")

		FILE_FORMAT = f"STG.{DATABASE_NAME}_{FILE_FORMAT}"

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
				FORMAT_NAME = {FILE_FORMAT}
			) 
			pattern = '.*{s3_csv_file}.*'
		"""
		SF_HOOK.run(sql_query)

	## STEP 5 : Create DAG operators

	start = DummyOperator(task_id="start", dag=creo_run)		# ! start, end = [EmptyOperator(task_id=tid, trigger_rule="all_done") for tid in ["start", "end"]]
	end = DummyOperator(task_id="end", trigger_rule="all_done", dag=creo_run) 	# ! end.trigger_rule = trigger_rule.TriggerRule.ALL_DONE

	trigger_DQ = TriggerDagRunOperator(
		task_id=f"trigger_DQ_{DATABASE_NAME}",
		trigger_dag_id=f"DQ_{DATABASE_NAME}",
		reset_dag_run=True,
		wait_for_completion=True,
		poke_interval=30,
		execution_date='{{ ds }}',
		dag=creo_run
	)

	## Execute ETL for each table
	for table_name in fullTableList: #! USE OF TABLE LIST DICTIONARY WILL BE DEPRECIATED
		task_group_id = f"task_group_{table_name}"
		
		with TaskGroup(group_id=task_group_id) as pandas_task_group:
			runtime_params = get_runtime_params(table_name)
			mssql_to_df = PythonOperator(
				task_id=f"export_mssql_df_{table_name}",
				python_callable=export_mssql_data,
				op_args=[runtime_params, table_name],
				dag=creo_run
			)
			df_to_s3 = PythonOperator(
				task_id=f"copy_to_s3_{table_name}",
				python_callable=load_mssql_df_to_s3,
				op_args=[runtime_params, table_name],
				dag=creo_run
			)
			s3_to_snowflake = PythonOperator(
				task_id=f"copy_to_sf_{table_name}",
				python_callable=copy_s3_to_snowflake,
				op_args=[runtime_params],
				dag=creo_run
			)
			mssql_to_df >> df_to_s3 >> s3_to_snowflake

	start >> pandas_task_group >> end >> trigger_DQ

	for table_name in fullTableList: #! IN DEVELOPMENT: Execute BCP & PIGZ task group for each table in tableList
		task_group_id = f"task_group_{table_name}"

		with TaskGroup(group_id=f"mssql_to_s3_{table_name}") as bcp_pigz_task_group: 
			fullTableList = SF_HOOK.run(f"SELECT {table_name} FROM ARES.ETL.UTILS_{DATABASE_NAME}") # todo : update snowflake hook to run query^

			mssql_to_csv = PythonOperator(
				task_id=f'export_csv_using_bcp_{DATABASE_NAME}_{table_name}',
				python_callable=export_csv_using_bcp,
				op_args=[runtime_params, table_name],
			)
			csv_to_zip = PythonOperator(
				task_id=f'zip_bcp_csv_using_pigz_{DATABASE_NAME}_{table_name}',
				python_callable=zip_bcp_csv_using_pigz,
				op_args=[runtime_params, table_name],
			)
			zip_to_s3 = PythonOperator(
				task_id=f'load_pigz_csv_to_s3_{DATABASE_NAME}_{table_name}',
				python_callable=load_pigz_csv_to_s3,
				op_args=[runtime_params, table_name],
			)
			s3_to_snowflake = PythonOperator(
				task_id=f"copy_to_sf_{table_name}",
				python_callable=copy_s3_to_snowflake,
				op_args=[runtime_params],
				dag=creo_run
			)
			mssql_to_csv >> csv_to_zip >> zip_to_s3 >> s3_to_snowflake

	# start >> bcp_pigz_task_group >> end >> trigger_DQ


creo_run = Staging_CREO()

