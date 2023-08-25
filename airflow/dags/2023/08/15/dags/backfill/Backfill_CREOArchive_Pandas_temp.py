import logging
import re
from datetime import datetime, timedelta
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.functions import ms_teams_callback_functions

# - - - - - - - - - SETTINGS - - - - - - - - -


author = 'Julie Scherer'

SNOWFLAKE_DATABASE = "ARES"
SNOWFLAKE_SCHEMA = "STG"
MSSQL_DATABASE = "CREO"
TABLE_LIST = ['DatasetRow', 'Global', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2']

COL_SEP = "|"
LINE_SEP = "\\n"
FILE_FORMAT = f"""
 TYPE = CSV 
 COMPRESSION = AUTO 
 FIELD_DELIMITER = '{COL_SEP}' 
 RECORD_DELIMITER = '{LINE_SEP}' 
 SKIP_HEADER = 0 
 REPLACE_INVALID_CHARACTERS = TRUE 
 
"""

CHUNK_SIZE = 20000 # Define the batch size for processing data in chunks

S3_ROOT = f"{MSSQL_DATABASE}/Backfill/TEST"

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
    """
    Returns a MSSQL hook for interacting with MSSQL.
    """
    MSSQL_HOOK = MsSqlHook(mssql_conn_id="awsmssql_creosql_creoarchive_conn")
    return MSSQL_HOOK

def get_s3_hook():
    """
    Returns a S3 hook for interacting with S3.
    """
    S3_HOOK = S3Hook(aws_conn_id="aws_s3_conn")
    return S3_HOOK

def get_sf_hook():
    """
    Returns a Snowflake hook for interacting with Snowflake.
    """
    SF_HOOK = SnowflakeHook(snowflake_conn_id="snowflake_default")
    return SF_HOOK

# - - - - - - - - SUPPORTING FUNCTIONS - - - - - - - - -

def get_table_definitions(mssql_table_name):
	sql_query = f"""
	SELECT DEFINITION
	FROM ARES.ETL.CREO_DDL
	WHERE TABLE_NAME = '{mssql_table_name}'
	"""
	ddl = get_sf_hook().get_pandas_df(sql_query)
	definitions = ddl['DEFINITION'].tolist() 
	schema = ', \n\t'.join(definitions)
	return schema

def format_copy_into_columns(mssql_table_name):
	sql_query = f"""
	SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
	FROM ARES.ETL.CREO_DDL
	WHERE TABLE_NAME = '{mssql_table_name}'
	"""
	ddl = get_sf_hook().get_pandas_df(sql_query)

	column_list = ddl['COLUMN_NAME'].tolist()
	data_types = ddl['DATA_TYPE'].tolist()
	null_data = ddl['NULLABLE'].tolist()

	formatted_columns = []
	for index, column_name in enumerate(column_list):
		column_number = index+1

		data_type = data_types[index]
		null = null_data[index]

		cast = re.sub(r'[^a-zA-Z_]', '', data_type).lower() # remove any characters that are not a letter or underscore, and turns to lowercase
		comment = f"\t-- ${column_number}: {column_name} {data_type} {null}" # comment to add at end of line
		
		formatted_col = f"(${column_number})::{cast}" if cast != 'timestamp_ltz' else f"to_timestamp_ntz(${column_number})" # cast the column to the correct data type
		formatted_col += f', {comment}' if (column_number < len(column_list)) else f' {comment}' # add comment with a preceeding comma, except if its the last column, then don't add a comma

		formatted_columns.append(formatted_col) # Append the single formatted column to the list

	return '\n'.join(formatted_columns)  # Join multiple formatted columns in the list with a newline and 2 tabs for formatting

# - - - - - - - - BACKFILL DAG - - - - - - - - -

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
    "Backfill_CREOArchive_Pandas",
    start_date=pendulum.datetime(2023, 1, 1, tz='US/Eastern'),
    catchup=False,
    schedule_interval="@once",
	dagrun_timeout=timedelta(hours=4),
    doc_md=__doc__,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
)
def Backfill_DAG_Pandas():

	@task(multiple_outputs=True)
	def get_runtime_params(mssql_table_name) -> dict:
		sf_table_name = f'{MSSQL_DATABASE}_{mssql_table_name}_HIST'

		file_name = f"PD_{mssql_table_name}_Backfill"
		logging.info(f"File name: {file_name}")

		fschema = get_table_definitions(mssql_table_name)
		logging.info(f"Formatted schema for creating {mssql_table_name} table: \n{fschema}")
		
		columns = format_copy_into_columns(mssql_table_name)
		logging.info(f"Formatted columns for loading staged data into {mssql_table_name}: \n{columns}")

		aod = datetime.strftime(datetime.now(), "%Y-%m-%d")
		
		s3_bucket_name = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001
		s3_tbl_path = f"{S3_ROOT}/{mssql_table_name}"

		runtime_params = {
			"mssql_table_name": mssql_table_name,
			"sf_table_name": sf_table_name,
			"file_name": file_name,
			"aod": aod,
			"fschema": fschema,
			"columns": columns,
			"s3_bucket_name": s3_bucket_name,
			"s3_tbl_path": s3_tbl_path,
		}

		return runtime_params
	
	@task
	def create_snowflake_table(runtime_params):
		sf_table_name = runtime_params.get("sf_table_name")
		fschema = runtime_params.get("fschema")
		# create_table_query = create_snowflake_table.format(sf_table_name=sf_table_name, fschema=fschema)
		create_table_query = f"""
		CREATE TABLE IF NOT EXISTS STG.{sf_table_name} ( 
			METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
			{fschema}
		);
		"""
		logging.info(f'CREATE TABLE: \n\n{create_table_query}')
		get_sf_hook().run(create_table_query)

	@task
	def export_pandas_batches(runtime_params):
		mssql_table_name = runtime_params.get("mssql_table_name")

		mssql_table_name = runtime_params.get("mssql_table_name")
		s3_bucket_name = runtime_params.get("s3_bucket_name")
		s3_tbl_path = runtime_params.get("s3_tbl_path")
		
		# dfs = []  # Initialize the list outside the loop
		mssql_query = f"SELECT * FROM [dbo].[{mssql_table_name}]"
		chunks = get_mssql_hook().get_pandas_df_by_chunks(sql=mssql_query, chunksize=CHUNK_SIZE)
		for idx, chunk in enumerate(chunks):
			df_byte = chunk\
				.to_csv(
					header=False,  # Exclude the column headers from the CSV
					index=False,  # Exclude the row index from the CSV
                    sep=f'{COL_SEP}',  # For example, use the pipe symbol as the column separator
                    lineterminator=f'{LINE_SEP}', # For example, use \n as the line seperator
                    na_rep='NULL',  # Replace missing values with 'NULL'
					compression='gzip',  # Compress the CSV file using gzip
					doublequote=True,  # Enable double quoting for values
					quotechar='"',
				).encode()
			
			# Load chunk to S3
			file_name = runtime_params.get("file_name")
			get_s3_hook().load_bytes(
				bytes_data=df_byte,
				bucket_name=s3_bucket_name,
				key=f"inbound/{s3_tbl_path}/{file_name}_{idx+1}.czv.gz",
				replace=True,
			)
			logging.info(f'Loading {file_name}_{idx+1}.csv.gz to S3')

	@task
	def s3_to_snowflake(runtime_params, batch=None):
		sf_table_name = runtime_params.get("sf_table_name")
		columns = runtime_params.get("columns")
		aod = runtime_params.get("aod")
		
		s3_tbl_path = runtime_params.get("s3_tbl_path")

		file_name = runtime_params.get("file_name")
		file_pattern = f"{file_name}_[0-9]+\.csv\.gz" # Example: '.*DatasetValue_Backfill_[0-9]+\.csv\.gz';

		copy_into_query = f"""
		-- \\ {sf_table_name}
		COPY INTO STG.{sf_table_name}
		FROM (
			SELECT 
				METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{aod}'), 
				{columns}
			FROM @ETL.INBOUND/{s3_tbl_path}/
		)
		FILE_FORMAT = ( 
			{FILE_FORMAT}
		) 
		PATTERN = '.*{file_pattern}.*'
		"""
		#load_staged_data.format(sf_table_name=sf_table_name, aod=aod, columns=columns, s3_tbl_path=s3_tbl_path, file_pattern=file_pattern)
		
		logging.info(f'COPY INTO: \n\n{copy_into_query}')
		get_sf_hook().run(copy_into_query)

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - 

	start = DummyOperator(task_id="start")
	end = DummyOperator(task_id="end", trigger_rule="all_done")
	
	for mssql_table_name in TABLE_LIST: 
		with TaskGroup(group_id=f"{mssql_table_name}_Task") as backfill_table:
			runtime_params = get_runtime_params(mssql_table_name)
			# runtime_params

			create_table_task = create_snowflake_table(runtime_params)
			export_pandas_batch_task = export_pandas_batches(runtime_params)
			s3_to_snowflake_task = s3_to_snowflake(runtime_params)
			create_table_task >> export_pandas_batch_task >> s3_to_snowflake_task

		start >> backfill_table >> end

backfill_dag = Backfill_DAG_Pandas()