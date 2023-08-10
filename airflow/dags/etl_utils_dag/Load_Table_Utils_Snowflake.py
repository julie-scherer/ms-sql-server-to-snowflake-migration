import os
import logging
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable

from include.functions import ms_teams_callback_functions

author = 'Julie Scherer'

## Global variables 
SNOWFLAKE_DATABASE = 'ARES'
SNOWFLAKE_SCHEMA = 'ETL'
MSSQL_DATABASE = 'CREO'
S3_BUCKET_NAME = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001
FILE_FORMAT = """
	TYPE = CSV
	FIELD_DELIMITER = '|'
	RECORD_DELIMITER = '\\n'
	SKIP_HEADER = 1
	NULL_IF = 'NULL'
"""
START_DATE = pendulum.datetime(2023, 7, 1, tz='US/Eastern')
TABLE_NAME = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{MSSQL_DATABASE}_UTILS_TEST"
LOCAL_OUT_DIR = f"include/UTILS/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}"
CSV_FILE_NAME = f"{MSSQL_DATABASE}_UTILS_TEST.csv"
S3_PATH = F"{MSSQL_DATABASE}/UTILS"

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
		"keys": "{'APP_VERSION'}",
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


# - - - - - - - - - - 

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

sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")


# - - - - - - - - - - 

default_args = {
	'owner': author,
	'start_date': datetime(2023, 7, 31),
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': timedelta(minutes=5),
}

@dag(
	'Load_Utils_to_Snowflake',
	start_date=START_DATE,
	catchup=False,
	schedule_interval='@once',
	dagrun_timeout=timedelta(hours=4),
	doc_md=__doc__,
	default_args=default_args,
	template_searchpath="/usr/local/airflow/include",
)
def Load_Utils_DAG():

	@task
	def create_snowflake_table():
		# get_sf_hook().run(f"DROP TABLE IF EXISTS {TABLE_NAME};")
		
		# Create Snowflake table
		query = f"""
		CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
			table_name VARCHAR NOT NULL
			,sql VARCHAR
			,key_column VARCHAR
			,keys TEXT
			,table_filtered_by VARCHAR -- use VARCHAR for CREO dag and BOOLEAN for CM & LD dags
		)
		"""
		get_sf_hook().run(query)
		logging.info(f"Create Snowflake table: \n{query}")
		return TABLE_NAME

	@task
	def df_to_s3():
		# Read the imported fullTableList dictionaries in as a Pandas df
		df = pd.DataFrame\
			.from_dict(
				fullTableList, 
				orient='index'
			)\
			.reset_index(level=0)\
			.rename(columns={'index':'table_name'})
		logging.info(f"Successfully read in table list: \n{df.head()}")

		# Export the pandas df as a CSV file in the include/ folder
		os.makedirs(LOCAL_OUT_DIR, exist_ok=True)
		file_path = os.path.join(LOCAL_OUT_DIR, CSV_FILE_NAME)

		df.to_csv(
			file_path,
			header=True,  # Data file needs to have a header row if using aql
			index=False,  # Exclude the row index from the CSV
			sep="|",  # Use the pipe symbol as the column separator
			na_rep='NULL',  # Replace missing values with 'NULL'
			doublequote=True,  # Enable double quoting for values
		)
		logging.info(f"Saved CSV file at {file_path}")

		# Load zip file to S3
		# https://airflow.apache.org/docs/apache-airflow/1.10.4/_api/airflow/hooks/S3_hook/index.html#airflow.hooks.S3_hook.S3Hook.load_file
		s3_path=f"inbound/{S3_PATH}/{CSV_FILE_NAME}"
		get_s3_hook().load_file(
			filename=file_path,
			key=s3_path,
			bucket_name=S3_BUCKET_NAME,
			replace=True,
		)
		logging.info(f"Successfully loaded CSV to S3: {s3_path}")

		# Remove files created locally
		os.remove(file_path)
		os.removedirs(LOCAL_OUT_DIR)
		logging.info(f"Removed files created locally")

	@task
	def s3_to_snowflake():
		get_sf_hook().run(f"TRUNCATE TABLE IF EXISTS {TABLE_NAME}")
		
		sql_query = f"""
			COPY INTO {TABLE_NAME}
			FROM (
				SELECT $1, $2, $3, $4, $5
				FROM @ETL.INBOUND/{S3_PATH}/
			)
			FILE_FORMAT = ( 
				{FILE_FORMAT}
			) 
			PATTERN = '.*{CSV_FILE_NAME}'    
		"""
		
		get_sf_hook().run(sql_query)
	

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - 

	start = DummyOperator(task_id="start")
	end = DummyOperator(task_id="end", trigger_rule="all_done")

	create_snowflake_table_task = create_snowflake_table()
	df_to_s3_task = df_to_s3()
	s3_to_snowflake_task = s3_to_snowflake()

	start >> create_snowflake_table_task >> df_to_s3_task >> s3_to_snowflake_task >> end

load_utils_dag = Load_Utils_DAG()

