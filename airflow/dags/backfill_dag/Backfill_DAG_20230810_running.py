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
import math
import re
import time
import logging
from datetime import datetime, timedelta
import pendulum
import numpy as np

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

SNOWFLAKE_DATABASE = 'ARES'
SNOWFLAKE_SCHEMA = 'STG'
MSSQL_DATABASE = 'CREO'
TABLE_LIST = ['DatasetValue']
# TABLE_LIST = ['Message']

# MSSQL_DATABASE = 'CREOArchive'
# TABLE_LIST = ['DatasetCell', 'DatasetRow', 'Global', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2']

# MSSQL_DATABASE = 'CREOArchive2'
# TABLE_LIST = ['DatasetCell', 'DatasetRow', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2']

DELIMITER = '|'
FILE_FORMAT = \
f"""TYPE = CSV
    COMPRESSION = AUTO
    FIELD_DELIMITER = '{DELIMITER}'
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 0
"""

# Define the batch size for processing data in chunks
BATCH_SIZE = 1000

# - - - - - - CONNECTIONS & HOOKS - - - - - - -

def get_mssql_hook():
	"""
	Returns a MSSQL hook for interacting with MSSQL.
	"""
	MSSQL_CONN = "awsmssql_creosql_creo_conn"
	# MSSQL_CONN = mssql_connection_params["conn_id"]
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

# - - - - - - - - STAGING DAG - - - - - - - - -

def get_table_definitions(mssql_table_name):
	sql_query = f"""
	SELECT Column_Data FROM ETL.{MSSQL_DATABASE}_DDL
	WHERE Table_Name = '{mssql_table_name}'
	"""
	# ddl = get_sf_hook().get_records(sql_query)
	ddl = get_sf_hook().get_first(sql_query)
	col_data = ddl.get('COLUMN_DATA')
	return col_data

def format_copy_into_columns(col_data):
	pretty_col_data = col_data.replace('"en-ci"',"'en-ci'").replace('"','')
	col_list = re.split(r'(?<![0-9]),', pretty_col_data)

	formatted_columns = []
	for index, column in enumerate(col_list, start=1):
		column_data = column.strip().split(' ') # remove any trailing white spaces on the left or right ends of the string, and then split the string into a list
		col_name, col_type = column_data[0], column_data[1]

		cast = re.sub(r'[^a-zA-Z_]', '', col_type).lower() # remove any characters that are not a letter or underscore, and turns to lowercase
		cmt = f"\t-- ${index}: {col_name} {col_type} {'NOT NULL' if 'NOT' in column_data else 'NULL'}" # comment to add at end of line
		
		formatted_col = f"(${index})::{cast}" if cast != 'timestamp_ltz' else f"to_timestamp_ntz(${index})" # cast the column to the correct data type
		formatted_col += f', {cmt}' if (index < len(col_list)) else f' {cmt}' # add comment with a preceeding comma, except if its the last column, then don't add a comma

		formatted_columns.append(formatted_col) # Append the single formatted column to the list

	return '\n'.join(formatted_columns)  # Join multiple formatted columns in the list with a newline and 2 tabs for formatting

def format_ddl_schema(col_data):
	col_data = col_data.replace('"en-ci"',"'en-ci'").replace(' ,',',').replace('"','')
	col_data = re.sub(r'(?<![0-9]),', ',\n\t', col_data)
	return col_data

# Run MSSQL query to get Primary Key Column
def get_primary_key(mssql_table_name):
	# Get the primary key column(s) of the table to order by
	query = f"""
	SELECT COLUMN_NAME
	FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
	WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
	AND TABLE_CATALOG = '{MSSQL_DATABASE}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{mssql_table_name}';
	"""
	df = get_mssql_hook().get_pandas_df(query)
	primary_key = df.iloc[0,0]
	return primary_key

def get_row_count(mssql_table_name):
	# Get the total count of rows in the MSSQL table
	# query = f"SELECT COUNT(*) FROM [dbo].[{mssql_table_name}]"
	query = f"""
	SELECT p.rows AS [RowCount]
	FROM sys.tables t
	JOIN sys.partitions p ON t.object_id = p.object_id
	WHERE p.index_id IN (0, 1)
	AND t.name = '{mssql_table_name}';
	"""
	df = get_mssql_hook().get_pandas_df(query)
	total_rows = df.iloc[0,0]
	return total_rows

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
	"Backfill_DAG",
	start_date=pendulum.datetime(2023, 7, 1, tz='US/Eastern'),
	catchup=False,
	schedule='@once',  # 6AM EST
	dagrun_timeout=timedelta(hours=4),
	doc_md=__doc__,
	template_searchpath=f"include/sql/Staging_{MSSQL_DATABASE}/",
	default_args=default_args
)
def Backfill_DAG():

	@task(multiple_outputs=True)
	def get_runtime_params(mssql_table_name) -> dict:
		sf_table_name = f'{MSSQL_DATABASE}_{mssql_table_name}_HIST'

		col_data = get_table_definitions(mssql_table_name)
		fschema = format_ddl_schema(col_data)
		logging.info(f"Formatted schema for creating {mssql_table_name} table: \n{fschema}")
		columns = format_copy_into_columns(col_data)
		logging.info(f"Formatted columns for loading staged data into {mssql_table_name}: \n{columns}")

		# columns = get_table_definitions(mssql_table_name)
		# logging.info(f"Formatted columns for loading staged {mssql_table_name} data: \n{columns}")

		primary_key = get_primary_key(mssql_table_name)
		logging.info(f"Primary key: {primary_key}")
		
		row_count = get_row_count(mssql_table_name)
		logging.info(f"Number of rows: {row_count}")

		num_iterations = math.ceil(row_count / BATCH_SIZE)
		logging.info(f"Number of iterations: {num_iterations}")

		aod = datetime.strftime(datetime.now(), "%Y-%m-%d")
		
		s3_bucket_name = Variable.get("s3_etldata_bucket_var") #s3-dev-etldata-001
		s3_dir_path = f"{MSSQL_DATABASE}/Backfill/TEST/{mssql_table_name}"
		
		file_name = f"{mssql_table_name}_Backfill_Test"
		# csv_file_name = f"{mssql_table_name}_Backfill.csv"
		# zip_file_name = csv_file_name + '.gz'

		runtime_params = {
			"columns": columns,
			"fschema": fschema,
			"primary_key": primary_key,
			"row_count": row_count,
			"num_iterations": num_iterations,
			"sf_table_name": sf_table_name,
			"mssql_table_name": mssql_table_name,
			"aod": aod,
			"s3_bucket_name": s3_bucket_name,
			"s3_dir_path": s3_dir_path,
			"file_name": file_name,
			# "csv_file_name": csv_file_name,
			# "zip_file_name": zip_file_name,
		}

		return runtime_params
	
	@task
	def create_sf_table(runtime_params):
		sf_table_name = runtime_params.get("sf_table_name")
		fschema = runtime_params.get("fschema")
		create_table = f"""
		CREATE TABLE IF NOT EXISTS STG.{sf_table_name} ( 
		METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
		{fschema}
		);
		"""
		logging.info(f'CREATE TABLE: \n\n{create_table}')
		get_sf_hook().run(create_table)

	# @task
	def mssql_to_s3(runtime_params, offset, batch):
		"""
		Loads the ZIP file obtained from the previous task to the specified S3 bucket.
		"""
		primary_key = runtime_params.get("primary_key")

		mssql_table_name = runtime_params.get("mssql_table_name")
		s3_bucket_name = runtime_params.get("s3_bucket_name")
		s3_dir_path = runtime_params.get("s3_dir_path")
		

		logging.info(f'Exporting {mssql_table_name} from MSSQL')
		mssql_query = f"""
		SELECT * FROM [dbo].[{mssql_table_name}]
		ORDER BY {primary_key}
		OFFSET {offset} ROWS
		FETCH NEXT {BATCH_SIZE} ROWS ONLY;
		"""
		df = get_mssql_hook().get_pandas_df(sql=mssql_query)
		logging.info(f'MSSQL Query: \n\n{mssql_query}')
		
		if df.empty:
			logging.info(f'No results found')
			pass
		else:
			logging.info(f'Writing {mssql_table_name} in CSV format')
			df_byte = df\
				.to_csv(
					compression='gzip',  # Compress the CSV file using gzip
					sep=f'{DELIMITER}',  # Use the pipe symbol as the column separator
					index=False,  # Exclude the row index from the CSV
					na_rep='NULL',  # Replace missing values with 'NULL'
					header=False,  # Exclude the column headers from the CSV
					doublequote=True,  # Enable double quoting for values
					quotechar='"',
				)\
				.encode()
			# .replace({np.nan:'NULL'})\

			# Load zip file to S3
			# zip_file_name = runtime_params.get("zip_file_name")
			file_name = runtime_params.get("file_name")
			zip_file_name = f"{file_name}_{batch}.csv.gz" #file_name + str(batch) + '.csv.gz'
			logging.info(f'Loading {zip_file_name} to S3')
			get_s3_hook().load_bytes(
				bytes_data=df_byte,
				bucket_name=s3_bucket_name,
				key=f"inbound/{s3_dir_path}/{zip_file_name}",
				replace=True,
			)
		
		return df

	# @task
	def s3_to_snowflake(runtime_params, batch):
		sf_table_name = runtime_params.get("sf_table_name")
		columns = runtime_params.get("columns")
		aod = runtime_params.get("aod")
		
		s3_bucket_name = runtime_params.get("s3_bucket_name")
		s3_dir_path = runtime_params.get("s3_dir_path")

		# zip_file_name = runtime_params.get("zip_file_name")
		file_name = runtime_params.get("file_name")
		zip_file_name = f"{file_name}_{batch}.csv.gz"
		load_staged_data = f"""
		-- \\ {sf_table_name}
		COPY INTO STG.{sf_table_name} 
		FROM (
		SELECT 
			METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{aod}'), 
			{columns} 
		FROM @ETL.INBOUND/{s3_dir_path}/
		)
		FILE_FORMAT = ( 
			{FILE_FORMAT}
		) 
		PATTERN = '.*{zip_file_name}.*'
		"""
		logging.info(f'COPY INTO: \n\n{load_staged_data}')
		get_sf_hook().run(load_staged_data)


	@task
	def run_backfill_pipeline(runtime_params):
		mssql_table_name = runtime_params.get("mssql_table_name")
		logging.info(f"Running backfill for {mssql_table_name}")
		num_iterations = runtime_params.get("num_iterations")
		for i in range(num_iterations):
			# Start timer
			start = time.time()
			
			offset = i * BATCH_SIZE
			
			mssql_to_s3_task = mssql_to_s3(runtime_params, offset, i+1)
			s3_to_snowflake_task = s3_to_snowflake(runtime_params, i+1)

			mssql_to_s3_task
			s3_to_snowflake_task

			# Log end time
			end = time.time()
			exec_time = end - start 
			logging.info(f"Total time to execute backfill for batch {i+1}: {exec_time}")
		
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - 

	start = DummyOperator(task_id="start")
	end = DummyOperator(task_id="end", trigger_rule="all_done")
	
	for mssql_table_name in TABLE_LIST: 
		with TaskGroup(group_id=f"{mssql_table_name}_Task") as backfill_table:
			
			runtime_params = get_runtime_params(mssql_table_name)
			runtime_params

			create_table_task = create_sf_table(runtime_params)
			run_backfill_task = run_backfill_pipeline(runtime_params)
			
			create_table_task >> run_backfill_task

	start >> backfill_table >> end

backfill_dag = Backfill_DAG()
