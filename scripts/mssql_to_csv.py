## Imports
import os
import time
import logging
from datetime import datetime,timedelta
import concurrent.futures  # concurrent.futures module for parallel processing
import subprocess
import os
import shutil
import pyodbc # connect to mssql via pyodbc driver to get row count
import pandas as pd

from utils import Utils

# - - - - - - - - - SETTINGS - - - - - - - - - - -

## STEP 1. Set global configs
batches = Utils.BATCH # MSSQL databases and their tables
max_workers = 5
timeout = timedelta(seconds=90)
batch_size = 100000
server = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com' # Microsoft SQL server

# STEP 2. Define local directories and paths
local_dir = os.getcwd()
data_dir = os.path.join(local_dir, 'data')
logs_dir = os.path.join(local_dir, 'logs')
dst_dir = r'\\ictfs01\SharedUSA\IT\Batch\DW\BCP'

def make_dir(dir):
    os.makedirs(dir, exist_ok=True)
    return dir

# STEP 3. Create function to create and save logs
def log():
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

# - - - - - - - - - - - BCP - - - - - - - - - - - - -
# Function to create a BCP log file and return its path
def create_bcp_log_file(logs_dir, prefix):
    log_path = os.path.join(logs_dir, datetime.datetime.now().strftime(f'{prefix}_%Y%m%d_%H%M_Log.txt'))
    return log_path


# Function to get the primary key column(s) for the specified table
def get_primary_key_columns(database_name, table_name, cursor):
    # You can use your database's specific query based on its schema (e.g., SQL Server)
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_CATALOG = '{database_name}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
    """
    # Execute the query and get the primary key column(s)
    primary_key_columns = cursor.execute(query)
    print(f"Primary key columns in {table_name}: {primary_key_columns}")

    # Return the primary key column(s) as a list of column names
    return [column_name for column_name in primary_key_columns]


# Function to run BCP export
def run_bcp_export(database_name, table_name, csv_path, bcp_query, log_path, err_path, delimiter='|', linebreak=os.linesep):
    try:
        subprocess.run([
            "bcp",
            bcp_query,
            "queryout", csv_path, "-c",
            "-t" + delimiter, "-r" + linebreak,
            "-T", "-S", server,
            "-o", log_path,
            "-e", err_path,
        ], stderr=subprocess.PIPE)

        logging.info(f"Successfully executed BCP for {database_name}.dbo.{table_name}: {csv_path}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        logging.info(f">> Path: {csv_path}")
        return True

    except Exception as e:
        logging.error(f"Error occurred while exporting {database_name}.dbo.{table_name}: {e}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        return False


# Export CSV using BCP utility
def bcp_export_all_records(database_name, table_name, csv_path, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
    logging.info(f"Running BCP command for {database_name}.dbo.{table_name}... ")

    # create logs and error files
    with create_bcp_log_file(logs_dir, f'BCP_{database_name}_{table_name}') as bcp_path, \
         create_bcp_log_file(logs_dir, f'BCP_{database_name}_{table_name}') as err_path:
        
        bcp_query = f"""SELECT * FROM {database_name}.[dbo].[{table_name}];"""
        return run_bcp_export(database_name, table_name, csv_path, bcp_query, bcp_path, err_path, delimiter, linebreak)


# Export CSVs in batches
def bcp_export_batches(database_name, table_name, csv_path, offset_row_count=0, fetch_row_count=batch_size, delimiter='|', linebreak=os.linesep):
    # Use context manager (with) to create logs file
    with create_bcp_log_file(logs_dir, f'Batch_{offset_row_count}_{fetch_row_count}_BCP_{database_name}_{table_name}') as bcp_path, \
         create_bcp_log_file(logs_dir, f'Batch_{offset_row_count}_{fetch_row_count}_BCP_{database_name}_{table_name}') as err_path:
        
        # Get the primary key column(s) of the table from the database schema
        primary_key_columns = get_primary_key_columns(database_name, table_name)

        # Build the ORDER BY clause based on the primary key column(s)
        order_by_clause = ', '.join(primary_key_columns)

        # BCP query using OFFSET and FETCH to skip the first N rows and select the next N rows
        bcp_query = f"""
        SELECT * 
        FROM {database_name}.dbo.{table_name}
        ORDER BY {order_by_clause} ASC -- or DESC as needed
        OFFSET {offset_row_count} ROWS
        FETCH ROWS {fetch_row_count} ROWS ONLY
        """

        return run_bcp_export(database_name, table_name, csv_path, bcp_query, bcp_path, err_path, delimiter, linebreak)


# Main BCP function
def bcp_to_csv(database_name, table_name, csv_path, cursor):
    try:
        # Get row count for table
        row_count = cursor.execute(f'SELECT COUNT(*) FROM {database_name}.dbo.{table_name}')
        print(f"Row count for {table_name}: {row_count}")

        # Calculate the number of batches by dividing the row count by batch size (e.g., 100,000)
        num_batches = row_count // batch_size

        # Run BCP utility
        # >> option 1: if there's only 1 batch, run bcp_to_csv function
        if num_batches <= 1:
            bcp_export_all_records(database_name, table_name, csv_path)
            return  # exit the function when done

        # >> option 2: if there are multiple batches, run bcp export tasks in parallel
        batch_futures = []
        batch_workers = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_workers) as executor:
            # Submit tasks for each batch to the futures executor
            offset_row_count, fetch_row_count = 0, batch_size
            for batch in num_batches:
                future = executor.submit(bcp_export_batches, database_name, table_name, csv_path, num_batches)
                batch_futures.append(future)
                offset_row_count += batch_size + 1
                fetch_row_count += batch_size

            # Wait for the tasks to complete and handle exceptions
            concurrent.futures.wait(batch_futures)
            logging.info(f"Executing tasks with a timeout of {timeout.total_seconds()}.")

            for tbl_idx, batch_future in enumerate(concurrent.futures.as_completed(batch_futures, timeout=timeout.total_seconds())):
                try:
                    batch_future.result()
                except Exception as e:
                    logging.error(f'Error occurred in batch {tbl_idx+1}: {e}')

            # Handle the completion of the task
            for tbl_idx, batch_future in enumerate(batch_futures):
                if batch_future.done() and not future.cancelled():
                    logging.info(f'Batch {tbl_idx+1} completed successfully.')
                elif batch_future.done() and batch_future.cancelled():
                    logging.warning(f'Batch {tbl_idx+1} was cancelled.')
                else:
                    logging.error(f'Batch {tbl_idx+1} did not complete.')

    except Exception as e:
        logging.error(f'Error occurred in the main BCP function: {e}')


# - - - - - - - - - - - REPLACE NAN - - - - - - - - - - - - -
def replace_nan(csv_path):
    logging.info(f"Replacing empty strings and NaN with NULL: {csv_path}")
    try:
        # Replace empty strings and whitespace with NaN while reading
        na_values = ['', ' ']
        df = pd.read_csv(csv_path, na_values=na_values)

        # Replace NaN with 'NULL'
        df.fillna('NULL', inplace=True)

        # Save back to CSV
        df.to_csv(csv_path, index=False)
    
        logging.info(f"Successfully updated NULL values: {csv_path}")

    except Exception as e:
        logging.error(f"Error occurred while updating NULL values for {csv_path}: {e}")


# - - - - - - - - - - - PIGZ - - - - - - - - - - - - -
def pigz_file(csv_path):
    logging.info(f"Running PigZ: {csv_path}")
    try:
        proc = subprocess.Popen(
            f'pigz {csv_path}'.split(maxsplit=1),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if err != b'':
            raise ValueError(f'The following error occured while running pigZ {err}')
        
        if os.path.exists(csv_path + '.gz') is False:
            raise ValueError(f"Zip file does not exist: {csv_path+'.gz'}")
        
        logging.info(f"Successfully executed PigZ command: {csv_path+'.gz'}")
        return True
    
    except Exception as e:
        logging.error(f"Error occurred while running PigZ for {csv_path}: {e}")
        return False


# - - - - - - - - - LOCAL > CLOUD - - - - - - - - - - -
def move_zip(src_tbl_dir, dst_tbl_dir, csv_file):
    logging.info(f"Moving {csv_file+'.gz'} to {dst_tbl_dir} and removing {csv_file} in {src_tbl_dir}... ")
    try:
        src_zip = os.path.join(src_tbl_dir, csv_file + ".gz")
        dst_zip = os.path.join(dst_tbl_dir, csv_file + ".gz")
        if os.path.exists(dst_zip):
            subprocess.call(f'del {dst_zip}', shell=True)
            logging.info(f"Deleted zip file in destination directory: {dst_zip}")
        if os.path.exists(src_zip) and not os.path.exists(dst_zip):
            subprocess.call(f'move {src_zip} {dst_zip}', shell=True)
            logging.info(f"Successfully moved {src_zip} to {dst_zip}")
            logging.info(f">> File Size: {os.path.getsize(dst_zip)} bytes")
            logging.info(f">> Path: {dst_zip}")
        if os.path.exists(src_zip) and os.path.exists(dst_zip):
            subprocess.call(f'rm -r {src_tbl_dir}', shell=True)
            subprocess.call(f'del {src_tbl_dir}', shell=True)
            logging.info(f"Deleted source directory: {src_tbl_dir}")
        return True
    
    except Exception as e:
        logging.error(f"Error occurred while moving zip file: {e}")
        return False


# - - - - - - - - BCP > PIGZ > MOVE - - - - - - - - - -
def export_pipeline(database_name, table_name, cursor):
    src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name))
    dst_tbl_dir = make_dir(os.path.join(dst_dir, database_name, table_name))
    csv_filename = f"{table_name}_Backfill.csv"
    csv_path = os.path.join(src_tbl_dir, csv_filename)

    bcp_to_csv(database_name, table_name, csv_path, cursor)
    replace_nan(csv_path)
    pigz_file(csv_path)
    move_zip(src_tbl_dir, dst_tbl_dir, csv_filename)

    logging.info(f"Finished {database_name}.dbo.{table_name}")


# - - - - - - - - - - - RUN APP - - - - - - - - - - - - -
def run(databases: list[tuple]):
    for idx, database in enumerate(databases):
        # get the database and table names from the tuple
        database_name, table_names = database[0], database[1]

        logging.info(f"Running batch {idx+1} >> {database_name}")

        # create pyodbc connection for the server & database
        conn = pyodbc.connect('Driver={SQL Server};'
                            f'Server={server};'
                            f'Database={database_name};'
                            'Trusted_Connection=yes;')
        cursor = conn.cursor()

        # Create an empty array to store "futures" tasks
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

            # Submit tasks for each table in the database to the futures executor
            for idx, table_name in enumerate(table_names):
                future = executor.submit(export_pipeline, database_name, table_name, cursor)
                futures.append(future)
                logging.info(f"Task {idx+1} submitted to executor >> {database_name}.dbo.{table_name}")
            
             # Wait for the tasks to complete
            concurrent.futures.wait(futures)
            logging.info(f"Executing tasks with a timeout of {timeout.total_seconds()}")

            # Execute tasks
            for future in concurrent.futures.as_completed(futures, timeout=timeout.total_seconds()):
                future.result()
            
            # Handle the completion of the task
            for tbl_idx, future in enumerate(futures): 
                if future.done() and not future.cancelled():
                    logging.info(f'Task {tbl_idx+1} completed successfully.')
                elif future.done() and future.cancelled():
                    logging.warning(f'Task {tbl_idx+1} was cancelled.')
                else:
                    logging.error(f'Task {tbl_idx+1} did not complete.')


# - - - - - - - - EXECUTE WITH LOGGING - - - - - - - - - -
def main():
    print("*******************************************\n"
          f"                 Running...               \n"
          "*******************************************\n")
    
    make_dir(logs_dir)
    log()
    
    start = time.time()
    start_datetime = datetime.now()
    logging.info(f"Start time: {start_datetime.hour}:{start_datetime.minute}:{start_datetime.second}")
    
    run(batches)
    
    end = time.time()
    end_datetime = datetime.now()
    logging.info(f"End time: {end_datetime.hour}:{end_datetime.minute}:{end_datetime.second}")
    
    exec_time = end - start
    logging.info(f'Total time to execute: {round(exec_time,0)} seconds | {exec_time//60} minutes')
    
    print("\n*******************************************\n"
          f"                 Finished!                \n"
          "*******************************************\n")

if __name__ == "__main__":
    main()
