## Imports
import os
import time
import logging
from datetime import datetime, timedelta
import concurrent.futures  # concurrent.futures module for parallel processing
import subprocess
import os
import pyodbc # connect to mssql via pyodbc driver to get row count
import pandas as pd

from utils import Utils


# - - - - - - - - - SETTINGS - - - - - - - - - - -

## STEP 1. Set global configs
batches = Utils.BATCH # MSSQL databases and their tables
server = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com' # Microsoft SQL server
max_workers = 5
timeout = timedelta(seconds=90)

# >> choose your batch size
# batch_size = 1000000 # 1 mil records
# batch_size = 100000 # 100k records
batch_size = 10000 # 10k records
# batch_size = 5000 # 5k records

start_batch_idx = 1
'''
ENDED AT MessageDeliveryStatus_Backfill_755 
'''


# >> file format
# delimiter='|'
# linesep='`"\r'

# >> try different encodings here
# bcp_encoding='ACP'
# pd_encoding='cp1252'
# encoding='utf-8'
# encoding='utf-16'
# encoding='utf-8-sig'
# encoding='iso_1'
# encoding='latin-1'
# encoding='ISO-8859-1'

# bcp_parallel_is_true = True # / False
# bcp_replace_nan_is_true = True # / False

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
    runs_dir = make_dir(os.path.join(logs_dir, 'runs'))
    log_file_name = datetime.now().strftime(f'Run_%Y%m%d_%H%M.txt')
    log_file_path = os.path.join(runs_dir, log_file_name)
    logging.basicConfig(
        filename=log_file_path,
        format='[%(levelname)s] %(asctime)s:  %(message)s',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    print(f"Log file saved at {log_file_path}")


# - - - - - - - - - - - BCP - - - - - - - - - - - - -

# ** Supporting functions
# >> Function to create a BCP log file and return its path
def create_bcp_log_file(database_name, table_name, type_folder, filename,):
    """
    Create a BCP log file with a given prefix and return its path.

    Parameters:
        dir (str): Directory where the log file will be created.
        prefix (str): Prefix for the log file name.

    Returns:
        str: Path to the created BCP log file.
    """
    bcp_logs = make_dir(os.path.join(logs_dir, 'bcp'))
    type_logs = make_dir(os.path.join(bcp_logs, type_folder))
    todays_logs = make_dir(os.path.join(type_logs, datetime.now().strftime(f'%Y_%m_%d')))
    db_logs = make_dir(os.path.join(todays_logs, database_name))
    tbl_logs = make_dir(os.path.join(db_logs, table_name))
    file = os.path.join(tbl_logs, datetime.now().strftime(f'{filename}.txt'))
    return file


# >> Function to get the primary key column(s) for the specified table
def get_primary_key_columns(database_name, table_name, cursor):
    """
    Get the primary key column(s) for the specified table.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        cursor: PyODBC cursor object to execute the query.

    Returns:
        list: List of primary key column names.
    """
    # You can use your database's specific query based on its schema (e.g., SQL Server)
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
    AND TABLE_CATALOG = '{database_name}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
    """
    # Execute the query and get the primary key column(s)
    cursor.execute(query)
    primary_key_columns = [row.COLUMN_NAME for row in cursor.fetchall()]  # Fetch all rows and extract the COLUMN_NAME

    # print(f"Primary key columns in {table_name}: {primary_key_columns}")

    # Return the primary key column(s) as a list of column names
    return primary_key_columns



# ** Main BCP function that's called first
# >> Checks how many records are in the table and splits into batches if there are more records than the batch size
def run_bcp_main(database_name, table_name, cursor):
    """
    Run the BCP utility to export data from a table to CSV files.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        src_tbl_dir (str): Source directory to save CSV files.
        cursor: PyODBC cursor object to execute queries.

    Returns:
        list: List of paths to the exported CSV files.
    """
    src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name))
    dst_tbl_dir = make_dir(os.path.join(dst_dir, database_name, table_name))
    
    # try:
    # Get row count for table
    cursor.execute(f'SELECT COUNT(*) FROM {database_name}.[dbo].[{table_name}];')
    row_count = cursor.fetchone()[0]  # Fetch the first column value from the first row
    # print(f"Row count for {table_name}: {row_count}, {type(row_count)}")

    # Calculate the number of batches by dividing the row count by batch size (e.g., 100,000)
    num_batches = row_count // batch_size

    # Run BCP utility

    # >> OPTION 1: if there's only 1 batch, export all records into 1 file
    
    if num_batches <= 1:
        logging.info(f"Running BCP command to export all records in {database_name}.dbo.{table_name}:")
        
        # create logs and error files
        bcp_log_file = create_bcp_log_file(database_name, table_name, 'output', f'BCP_Out_{database_name}_{table_name}')
        bcp_err_file = create_bcp_log_file(database_name, table_name, 'errors', f'BCP_Err_{database_name}_{table_name}')

        csv_filename = f"{table_name}_Backfill.csv"
        csv_path = os.path.join(src_tbl_dir, csv_filename)

        bcp_query = bcp_export_all_query(database_name, table_name, csv_path)

        bcp_pigz_move(database_name, table_name, csv_path, bcp_query, bcp_log_file, bcp_err_file, src_tbl_dir, dst_tbl_dir)
        
        return

    # >> OPTION 2: if there are multiple batches...

    logging.info(f"Running BCP command to export {database_name}.dbo.{table_name} in batches:")

    # Get the primary key column(s) of the table from the database schema
    primary_key_columns = get_primary_key_columns(database_name, table_name, cursor)
    # Build the ORDER BY clause based on the primary key column(s)
    order_by = ', '.join(primary_key_columns)

    batch_futures = []
    batch_workers=5
    offset_row_count = 0
    fetch_row_count = batch_size
    # with concurrent.futures.ThreadPoolExecutor(max_workers=batch_workers) as executor:
    #     # Submit tasks for each batch to the futures executor
    for batch_num in range(start_batch_idx,num_batches+1):
        # create logs and error files
        bcp_log_file = create_bcp_log_file(database_name, table_name, 'output', f'BCP_Out_{database_name}_{table_name}_B{batch_num}')
        bcp_err_file = create_bcp_log_file(database_name, table_name, 'errors', f'BCP_Err_{database_name}_{table_name}_B{batch_num}')

        # create file
        csv_filename = f"{table_name}_Backfill_{batch_num}.csv"
        csv_path = os.path.join(src_tbl_dir, csv_filename)

        logging.info(f"Batch {batch_num}: {offset_row_count} - {fetch_row_count}")
        bcp_query = bcp_export_batches_query(database_name, table_name, order_by, offset_row_count, fetch_row_count)

        bcp_pigz_move(database_name, table_name, csv_path, bcp_query, bcp_log_file, bcp_err_file, src_tbl_dir, dst_tbl_dir, batch_num)
        # future = executor.submit(bcp_pigz_move, database_name, table_name, csv_path, bcp_query, bcp_log_file, bcp_err_file, src_tbl_dir, dst_tbl_dir, batch_num)
        # batch_futures.append(future)

        # update offset and fetch row count
        offset_row_count += batch_size
        # fetch_row_count += batch_size

        # # Wait for the tasks to complete and handle exceptions
        # concurrent.futures.wait(batch_futures)
        # logging.info(f"Executing tasks with a timeout of {timeout.total_seconds()}.")

        # for batch_future in concurrent.futures.as_completed(batch_futures, timeout=timeout.total_seconds()):
        #     try:
        #         batch_future.result()
        #     except Exception as e:
        #         logging.error(f"Error occurred while executing task for batch {batch_num}: {e}")

        # return


# * If there's less records than the batch size:
# >> Export all the records into 1 CSV using BCP
def bcp_export_all_query(database_name, table_name):
    """
    Export all records from a table to a CSV file using BCP.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        csv_path (str): Path to the CSV file to export.

    Returns:
        str: Path to the exported CSV file.
    """

    bcp_query = f"""SELECT * FROM {database_name}.[dbo].[{table_name}];"""
    logging.info(f"BCP query for {database_name}.dbo.{table_name}: \n{bcp_query}")

    return bcp_query


# * If there's more records than the batch size, run in batches:
# * Export CSVs in batches using OFFSET and FETCH NEXT
def bcp_export_batches_query(database_name, table_name, order_by, offset_row_count, fetch_row_count):
    """
    Export records from a table to a CSV file using BCP in batches.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        csv_path (str): Path to the CSV file to export.
        order_by (str): Column(s) to use for ordering the data.
        offset_row_count (int): Number of rows to skip (OFFSET).
        fetch_row_count (int): Number of rows to fetch (FETCH NEXT).

    Returns:
        str: Path to the exported CSV file.
    """

    # BCP query using OFFSET and FETCH to skip the first N rows and select the next N rows
    bcp_query = f"""
    SELECT * 
    FROM {database_name}.[dbo].[{table_name}]
    ORDER BY {order_by} ASC
    OFFSET {str(offset_row_count)} ROWS
    FETCH NEXT {str(fetch_row_count)} ROWS ONLY
    """
    # print(bcp_query)
    logging.info(f"BCP query for {database_name}.dbo.{table_name}: \n{bcp_query}")

    return bcp_query


# >> Function to run BCP utility command
def bcp_utility_cmd(database_name, table_name, csv_path, bcp_query, log_path, err_path, delimiter='|', linebreak='os.linesep', codepage="65001", header=False):
    """
    Run the BCP export utility to export data to a CSV file.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        csv_path (str): Path to the CSV file to export.
        bcp_query (str): BCP query to export data from the table.
        log_path (str): Path to the BCP log file.
        err_path (str): Path to the BCP error file.

    Returns:
        bool: True if BCP export is successful, False otherwise.
    """
    try:
        subprocess.run([
            "bcp",
            bcp_query,
            "queryout", csv_path, 
            "-T",  # Trusted connection
            "-c", # Character encoding (UTF-8)
            "-t" + delimiter, 
            "-r" + linebreak,
            "-S", server,
            "-d", database_name,
            "-o", log_path,
            "-e", err_path,
            "-k",
        ], stderr=subprocess.PIPE)

        return True

    except Exception as e:
        logging.error(f"Error occurred while executing BCP utility for {database_name}.dbo.{table_name}: {e}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        return False


# - - - - - - - - BCP > PIGZ > MOVE - - - - - - - - - -
# ** All tasks are bunched into one future so they are executed together
def bcp_pigz_move(database_name, table_name, csv_path, bcp_query, log_path, err_path, src_tbl_dir, dst_tbl_dir, batch_num=0):
    exported = bcp_utility_cmd(database_name, table_name, csv_path, bcp_query, log_path, err_path)

    if exported and os.path.exists(csv_path):
        logging.info(f"Successfully exported {database_name}.dbo.{table_name} using BCP:")
        logging.info(f">> File Location: {csv_path}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")

        # replace_nan(csv_path) ## ! use this to replace nan values in parallel with bcp export
        pigz_file(csv_path)
        csv_file = f"{table_name}_Backfill_{batch_num}.csv" if batch_num else f"{table_name}_Backfill.csv"
        move_zip(src_tbl_dir, dst_tbl_dir, csv_file)

    logging.info(f"Finished {database_name}.dbo.{table_name}")


# - - - - - - - - - - - REPLACE NAN - - - - - - - - - - - - -
def replace_nan(csv_path):
    logging.info(f"Replacing empty strings and NaN with NULL: {csv_path}")
    try:
        # Replace empty strings and whitespace with NaN while reading
        # na_values = ['', ' '] # snowflake expects 'NULL'
        try:
            df = pd.read_csv(
                csv_path,
                # io.StringIO(csv_path), 
                # engine='python',
                header=None,
                sep="|",
            )
            logging.info(f"Successfully read CSV")
            print(f"read df: \n{df}")
        except Exception as e:
            return logging.error(f"Pandas error! Unable to read CSV: {e}")

        # Replace NaN with 'NULL'
        df.fillna('NULL', inplace=True).encode()
        print(f"fill na: \n{df}")

        try:
            # Save back to CSV
            df.to_csv( 
                csv_path,  
                header=False,  # Exclude the column headers from the CSV
                index=False,  # Exclude the row index from the CSV
                sep="|",  # Use the pipe symbol as the column separator
                na_rep='NULL',  # Replace missing values with 'NULL'
                compression='gzip',  # Compress the CSV file using gzip
                doublequote=True,  # Enable double quoting for values

                # index=False, 
                # header=False, 
                # sep='|', 
                # na_rep='NULL' 
            )
        except Exception as e:
            return logging.error(f"Pandas error! Unable to export csv: {e}")

        logging.info(f"Successfully updated NULL values: {csv_path}")

    except pd.errors.EmptyDataError:
        logging.warning(f"The CSV file is empty: {csv_path}")
    except Exception as e:
        logging.error(f"Error occurred while updating NULL values for {csv_path}: {e}")

    finally:
        df = None  # Release the DataFrame


# - - - - - - - - - - - PIGZ - - - - - - - - - - - - -
def pigz_file(csv_path):
    logging.info(f"Running PigZ: {csv_path}")
    try:
        if os.path.exists(csv_path+'.gz'):
            logging.info(f"Found zip file in local directory, deleting existing file: {csv_path+'.gz'}")
            subprocess.call(f"del {csv_path+'.gz'}", shell=True)
        
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
    logging.info(f"Attempting to move {csv_file+'.gz'}:")
    try:
        src_zip = os.path.join(src_tbl_dir, csv_file + ".gz")
        logging.info(f">> Source zip file: {src_zip}")
        
        dst_zip = os.path.join(dst_tbl_dir, csv_file + ".gz")
        logging.info(f">> Destination zip file: {dst_zip}")

        if os.path.exists(dst_zip):
            subprocess.call(f'del {dst_zip}', shell=True)
            logging.info(f"Found zip file in destination directory, deleting existing file: {dst_zip}")
        
        if os.path.exists(src_zip) and not os.path.exists(dst_zip):
            subprocess.call(f'move {src_zip} {dst_zip}', shell=True)
            logging.info(f"Successfully moved {src_zip} to {dst_zip}")
            logging.info(f">> File Size: {os.path.getsize(dst_zip)} bytes")
            logging.info(f">> Path: {dst_zip}")
        
        if os.path.exists(src_zip) and os.path.exists(dst_zip):
            subprocess.call(f'rm -r {src_tbl_dir}', shell=True)
            subprocess.call(f'rmdir {src_tbl_dir}', shell=True)
            logging.info(f"Deleted source directory: {src_tbl_dir}")
        
        logging.info(f"Move successful")
        return True
    
    except Exception as e:
        logging.error(f"Error occurred while moving zip file: {e}")
        return False



# - - - - - - - - - - - RUN APP - - - - - - - - - - - - -
def run(databases: list[tuple]):
    for idx, database in enumerate(databases):
        # get the database and table names from the tuple
        database_name, table_names = database[0], database[1]

        logging.info(f"Running batch {idx+1} >> {database_name}")

        # create replace connection for the server & database
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
                future = executor.submit(run_bcp_main, database_name, table_name, cursor)
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
