## Python library
import os
import time
import logging
from datetime import datetime, timedelta
import concurrent.futures  # concurrent.futures module for parallel processing
import subprocess
import os
import pyodbc # connect to mssql via pyodbc driver to get row count
## Local imports
from imports.export_utils import Utils
from imports.custom_bcp_sql_queries import sql_queries as csql

# - - - - - - - - - SETTINGS - - - - - - - - - - -

## STEP 1. Set global configs
batches = Utils.BATCH # MSSQL databases and their tables
server = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com' # Microsoft SQL server
max_workers = 5 # // max number of workers for parallel processing
timeout = timedelta(seconds=90) # // set time limit on submitting concurrent tasks to executor

# >> choose your default batch size (num records to export in each batch)
# default_batch_size = 1000000 # 1 mil records
default_batch_size = 100000 # 100k records
# default_batch_size = 50000 # 50k records
# default_batch_size = 10000 # 10k records
# default_batch_size = 5000 # 5k records

# >> custom settings defined in imports > utils file
available_row_counts = Utils.ROW_COUNTS
custom_batch_sizes = Utils.BATCH_SIZES
custom_start_batch_idx = Utils.START_IDX

# >> paramters used for testing
testing_small_batch = False # True = run only 2 batches
testing_bcp_only = False # True = only run BCP command to export to CSV locally

## STEP 2. Define local directories and paths
local_dir = os.getcwd()
data_dir = os.path.join(local_dir, 'data')
logs_dir = os.path.join(local_dir, 'logs')
shared_drive = r'\\ictfs01\SharedUSA\IT\Batch\DW\BCP'


## Supporting utilities
# >> Reusable function to quickly make directories if they don't exist
def make_dir(dir):
    os.makedirs(dir, exist_ok=True)
    return dir

# >> Function to create and save log file
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
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        type_folder (str): Folder where the log file will be stored.
        filename (str): Prefix for the log file name.

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

# >> Function to get the row count to split exports into batches
def get_row_count(database_name, table_name, cursor):
    """
    Get the number of rows in a table.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        cursor: PyODBC cursor object to execute the query.

    Returns:
        int: Number of rows in the table.
    """
    logging.info(f"Attempting to get the row count for {table_name}")
    try:
        # Get row count for table
        cursor.execute(f'SELECT COUNT(*) FROM {database_name}.[dbo].[{table_name}]')
        row_count = cursor.fetchone()[0]  # Fetch the first column value from the first row
        logging.info(f"Row count for {table_name}: {row_count}")
        return row_count
    
    except Exception as e:
        logging.error(f"Could not fetch row count for {table_name}: {e}")
        return

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
    logging.info(f"Attempting to get the primary key columns for {table_name}")
    try:
        # Get the primary key column(s) of the table to order by
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
        AND TABLE_CATALOG = '{database_name}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}';
        """

        # Execute the query and get the primary key column(s)
        cursor.execute(query)
        primary_key_columns = [row.COLUMN_NAME for row in cursor.fetchall()]  # Fetch all rows and extract the COLUMN_NAME

        logging.info(f"Query to get primay key column(s): \n{query}")
        logging.info(f"Primary key columns in {table_name}: {primary_key_columns}")

        # Build the ORDER BY clause based on the primary key column(s)
        order_by = ', '.join(primary_key_columns)

        # Return the primary key column(s) as a list of column names
        return order_by
    
    except Exception as e:
        logging.error(f"Could not fetch primary key columns to order by for {table_name}: {e}")
        return

# >> Function to run BCP utility command
def bcp_utility_cmd(database_name, table_name, csv_path, bcp_query, log_path, err_path, 
                    delimiter='^', linebreak='\n', codepage="65001", header=False):
    """
    Run the BCP export utility to export data to a CSV file.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        csv_path (str): Path to the CSV file to export.
        bcp_query (str): BCP query to export data from the table.
        log_path (str): Path to the BCP log file.
        err_path (str): Path to the BCP error file.
        delimiter (str): Field delimiter for the CSV file. Default is '^'.
        linebreak (str): Row delimiter for the CSV file. Default is '\\n'.
        codepage (str): Code page for the exported data. Default is "65001" (UTF-8).
        header (bool): Whether to include headers in the CSV file. Default is False.

    Returns:
        bool: True if BCP export is successful, False otherwise.
    """
    try:
        subprocess.run([
            "bcp",
            bcp_query,
            "queryout", csv_path, 
            "-c", # Character encoding (UTF-8)
            "-t" + delimiter, # field_term
            "-r" + linebreak, # row_term
            "-T",  # Trusted connection
            "-S", server, # server_name
            "-d", database_name, # database_name
            "-o", log_path, # output_file
            "-e", err_path, # err_file
            "-k", # application_intent
        ], stderr=subprocess.PIPE)

        if os.path.exists(csv_path):
            logging.info(f"Successfully exported {database_name}.dbo.{table_name} using BCP:")
            logging.info(f">> File Location: {csv_path}")
            logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        
            return True

    except Exception as e:
        logging.error(f"Error occurred while executing BCP utility for {database_name}.dbo.{table_name}: {e}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        return False

# >> Function to export all records into 1 CSV using BCP
def bcp_export_all_query(database_name, table_name):
    """
    Generate the BCP query to export all records from a table to a CSV file.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.

    Returns:
        str: BCP query to export all records.
    """
    custom_query = csql.get(f"{database_name}_{table_name}")
    if custom_query:
        # logging.info(f"Found custom BCP SQL query")
        bcp_query = custom_query
    else:
        bcp_query = f"""SELECT * FROM {database_name}.[dbo].[{table_name}];"""

    logging.info(f"Query to export all records for {database_name}.dbo.{table_name}: \n{bcp_query}")
    return bcp_query

# >> Function to export CSVs in batches using OFFSET and FETCH NEXT
def bcp_export_batches_query(database_name, table_name, order_by, offset_row_count, fetch_row_count, custom_sql=None):
    """
    Generate the BCP query to export records from a table to a CSV file in batches.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        order_by (str): Column(s) to use for ordering the data.
        offset_row_count (int): Number of rows to skip (OFFSET).
        fetch_row_count (int): Number of rows to fetch (FETCH NEXT).
        custom_sql (str, optional): Custom SQL query. Default is None.

    Returns:
        str: BCP query to export records in batches.
    """
    custom_query = csql.get(f"{database_name}_{table_name}_Batch")
    if custom_query:
        bcp_query = custom_query + f"""OFFSET {str(offset_row_count)} ROWS FETCH NEXT {str(fetch_row_count)} ROWS ONLY;
        """
    else:
        # BCP query using OFFSET and FETCH to skip the first N rows and select the next N rows
        bcp_query = f"""
        SELECT * 
        FROM {database_name}.[dbo].[{table_name}]
        ORDER BY {order_by}
        OFFSET {str(offset_row_count)} ROWS 
        FETCH NEXT {str(fetch_row_count)} ROWS ONLY;
        """

    logging.info(f"BCP query to export batch for {database_name}.dbo.{table_name}: \n{bcp_query}")
    return bcp_query

# >> Function to run the main BCP process
def run_bcp_main(database_name, table_name, order_by, num_batches, batch_size, src_tbl_dir, dst_tbl_dir):
    """
    Run the Python functions to run the BCP pipeline and export data from the MSSQL table into CSV format.

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        order_by (str): Column(s) to use for ordering the data.
        num_batches (int): Number of batches to export.
        batch_size (int): Number of rows per batch.
        src_tbl_dir (str): Source directory to save CSV files.
        dst_tbl_dir (str): Destination directory to move CSV files.

    Returns:
        None
    """
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

    # If we're starting at a custom batch number, we need to adjust start index, as well as the OFFSET value
    if custom_start_batch_idx.get(table_name):
        start_batch_idx = custom_start_batch_idx[table_name]
        offset_row_count = batch_size * (start_batch_idx-1)
        logging.info(f"Starting {database_name}.{table_name} at batch {start_batch_idx} with OFFSET = {offset_row_count}")
    else:
        start_batch_idx = 1
        offset_row_count = 0
    
    # FETCH value always remains constant ( equal to batch size ) 
    fetch_row_count = batch_size
    
    for batch_num in range(start_batch_idx, num_batches+1):
        # create logs and error files
        bcp_log_file = create_bcp_log_file(database_name, table_name, 'output', f'BCP_Out_{database_name}_{table_name}_B{batch_num}')
        bcp_err_file = create_bcp_log_file(database_name, table_name, 'errors', f'BCP_Err_{database_name}_{table_name}_B{batch_num}')

        # create file
        csv_filename = f"{table_name}_Backfill_{batch_num}.csv"
        csv_path = os.path.join(src_tbl_dir, csv_filename)

        logging.info(f"Batch {batch_num}: {offset_row_count} -> {offset_row_count+fetch_row_count}")
        bcp_query = bcp_export_batches_query(database_name, table_name, order_by, offset_row_count, fetch_row_count)
        bcp_pigz_move(database_name, table_name, csv_path, bcp_query, bcp_log_file, bcp_err_file, src_tbl_dir, dst_tbl_dir, batch_num)

        # update offset row count
        offset_row_count += batch_size


# - - - - - - - - - - - PIGZ - - - - - - - - - - - - -
def pigz_file(csv_path):
    """
    Compress the given CSV file using PigZ.

    Parameters:
        csv_path (str): Path to the CSV file to compress.

    Returns:
        bool: True if PigZ compression is successful, False otherwise.
    """
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
    """
    Move the compressed CSV file from the source directory to the destination directory.

    Parameters:
        src_tbl_dir (str): Source directory where the CSV file is located.
        dst_tbl_dir (str): Destination directory to move the CSV file.
        csv_file (str): Name of the compressed CSV file.

    Returns:
        bool: True if the move operation is successful, False otherwise.
    """
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


# - - - - - - - - BCP > PIGZ > MOVE - - - - - - - - - -
def bcp_pigz_move(database_name, table_name, csv_path, bcp_query, log_path, err_path, src_tbl_dir, dst_tbl_dir, batch_num=0):
    """
    Execute BCP utility to export data to a CSV file, compress the CSV using PigZ, 
    and move the compressed file. These three tasks are grouped together in one function, 
    and the concurrent futures module executes them for a given table before proceeding 
    to the next task (i.e., table).

    Parameters:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        csv_path (str): Path to the CSV file to export.
        bcp_query (str): BCP query to export data from the table.
        log_path (str): Path to the BCP log file.
        err_path (str): Path to the BCP error file.
        src_tbl_dir (str): Source directory where the CSV file is located.
        dst_tbl_dir (str): Destination directory to move the compressed CSV file.
        batch_num (int, optional): Batch number if exporting data in batches. Default is 0.

    Returns:
        None
    """
    exported = bcp_utility_cmd(database_name, table_name, csv_path, bcp_query, log_path, err_path)

    if testing_bcp_only:
        return 
    
    if exported:
        # replace_nan(csv_path) ## ! use this to replace nan values in parallel with bcp export
        pigz_file(csv_path)
        csv_file = f"{table_name}_Backfill_{batch_num}.csv" if batch_num else f"{table_name}_Backfill.csv"
        move_zip(src_tbl_dir, dst_tbl_dir, csv_file)

    logging.info(f"Finished {database_name}.dbo.{table_name}")



# - - - - - - - - - - - RUN APP - - - - - - - - - - - - -
def run(databases: list[tuple]):
    """
    Execute the main BCP export process for a list of databases and their tables.

    Parameters:
        databases (list[tuple]): List of tuples containing the database name and a list of table names.

    Returns:
        None
    """
    for idx, database in enumerate(databases):
        # get the database and table names from the tuple
        database_name, table_names = database[0], database[1]
        logging.info(f"Running batch {idx+1} >> {database_name}, {table_names}")

        # create replace connection for the server & database
        conn = pyodbc.connect('Driver={SQL Server};'
                            f'Server={server};'
                            f'Database={database_name};'
                            'Trusted_Connection=yes;')
        cursor = conn.cursor()

        # Only run 2 batches if testing small batch parameter is set to true
        if testing_small_batch:
            for idx, table_name in enumerate(table_names):
                src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name))
                dst_tbl_dir = make_dir(os.path.join(shared_drive, database_name, table_name))
                batch_size = custom_batch_sizes.get(table_name, default_batch_size)
                logging.info(f"Batch size for {table_name}: {batch_size}")
                num_batches = 2
                order_by = get_primary_key_columns(database_name, table_name, cursor)
                run_bcp_main(database_name, table_name, order_by, num_batches, batch_size, src_tbl_dir, dst_tbl_dir)
            cursor.close()
            conn.close()
            return

        # Otherwise, run the script using concurrent futures module to run the tasks in parallel
        futures = [] # Create an empty array to store "futures" tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, table_name in enumerate(table_names):
                src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name))
                dst_tbl_dir = make_dir(os.path.join(shared_drive, database_name, table_name))

                batch_size = custom_batch_sizes.get(table_name, default_batch_size)
                logging.info(f"Batch size for {table_name}: {batch_size}")

                # Check if approx row count available in Utils
                row_count = available_row_counts.get(table_name)
                if not row_count: # Otherwise, run a query to get the row counts
                    row_count = get_row_count(database_name, table_name, cursor)
                
                # Calculate the number of batches by dividing the row count by batch size (e.g., 100,000)
                num_batches = (row_count // batch_size)+1
                
                order_by = get_primary_key_columns(database_name, table_name, cursor)
                
                # Submit tasks for each table in the database to the futures executor
                future = executor.submit(run_bcp_main, database_name, table_name, order_by, num_batches, batch_size, src_tbl_dir, dst_tbl_dir)
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

        cursor.close()
        conn.close()


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
