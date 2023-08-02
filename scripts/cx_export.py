# pip install connectorx==0.3.2a2
import connectorx as cx
import logging
import subprocess
import concurrent.futures  # concurrent.futures module for parallel processing
import os
from datetime import datetime, timedelta

from imports.utils import Utils
from imports.custom_sql_queries import sql_queries as csql

server = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
batch1 = ('CREO', ['Contact', 'Message', 'MessageDeliveryStatus', 'MessagePartV2'] )
batch2 = ('CREOArchive', ['Global'])
batches = [ batch1, batch2 ]

shared_drive = r'\\ictfs01\SharedUSA\IT\Batch\DW\BCP'
data_dir = os.path.join(os.getcwd(), 'data')
logs_dir = os.path.join(os.getcwd(), 'logs')

max_workers = 10 # // max number of workers for parallel processing
timeout = timedelta(seconds=90) # // set time limit on submitting concurrent tasks to executor

# >> choose your default batch size (num records to export in each batch)
batch_size = 1000000 # 1 mil records
# batch_size = 100000 # 100k records
# batch_size = 50000 # 50k records
# batch_size = 10000 # 10k records
# batch_size = 5000 # 5k records

def make_dir(dir):
    os.makedirs(dir, exist_ok=True)
    return dir

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

def export_csv(database_name, table_name, df, csv_path):
    logging.info(f"Attempting to export CSV for {database_name} {table_name}")
    try:
        zip_file = csv_path + '.gz'
        df.to_csv(
            zip_file,  # Specify the output file path and name
            header=False,  # Exclude the column headers from the CSV
            index=False,  # Exclude the row index from the CSV
            sep="|",  # Use the pipe symbol as the column separator
            na_rep='NULL',  # Replace missing values with 'NULL'
            compression='gzip',  # Compress the CSV file using gzip
            doublequote=True,  # Enable double quoting for values
        )

        logging.info(f"Successfully exported {database_name} {table_name}: {zip_file}")
        return zip_file
    
    except Exception as e:
        logging.error(f"Unable to write {database_name} {table_name}")
        return None

# - - - - - - - - - LOCAL > DRIVE - - - - - - - - - - -
def move_zip(src_tbl_dir, dst_tbl_dir, zip_file):
    """
    Move the compressed CSV file from the source directory to the destination directory.

    Parameters:
        src_tbl_dir (str): Source directory where the CSV file is located.
        dst_tbl_dir (str): Destination directory to move the CSV file.
        csv_file (str): Name of the compressed CSV file.

    Returns:
        bool: True if the move operation is successful, False otherwise.
    """
    try:
        src_zip = os.path.join(src_tbl_dir, zip_file)
        logging.info(f">> Source zip file: {src_zip}")
        
        dst_zip = os.path.join(dst_tbl_dir, zip_file)
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


# - - - - - - - - - EXPORT > MOVE - - - - - - - - - - -
def export_zip_move(database_name, table_name, df, src_tbl_dir, dst_tbl_dir):
    num_batches = len(df) // batch_size + 1
    for i in range(num_batches):
        csv_path = os.path.join(src_tbl_dir, f"{table_name}_Backfill_{i+1}.csv")
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size
        print(f"Running batch {i+1} for {database_name} {table_name} starting at {start_idx} and ending at {end_idx}")
        batch_df = df.iloc[start_idx:end_idx]

        zip_file = export_csv(database_name, table_name, batch_df, csv_path)
        move_zip(src_tbl_dir, dst_tbl_dir, zip_file) if zip_file else None

    logging.info(f"Finished {database_name}.dbo.{table_name}")


def run(batches: list[tuple]):
    for idx, database in enumerate(batches):

        # get the database and table names from the tuple
        database_name, table_names = database[0], database[1]
        logging.info(f"Running {database_name}, {table_names}")

        futures = [] # Create an empty array to store "futures" tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, table_name in enumerate(table_names):
                src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name))
                dst_tbl_dir = make_dir(os.path.join(shared_drive, database_name, table_name))

                try:
                    logging.info(f"Reading in {database_name} {table_name}")
                    conn = f'mssql://{server}/{database_name}?trusted_connection=true' # connection token
                    query = f"""SELECT * FROM {database_name}.[dbo].[{table_name}];"""
                    df = cx.read_sql(conn, query)  # read data from MsSQL
                    logging.info(f"Successfully read in {database_name} {table_name}: \n{df.head()}")
                
                except Exception as e:
                    logging.error(f"Unable to read {database_name} {table_name}")
                    df = None
                    break

                # Submit tasks for each table in the database to the futures executor
                future = executor.submit(export_zip_move, database_name, table_name, df, src_tbl_dir, dst_tbl_dir)
                futures.append(future)
                logging.info(f"Task {idx+1} submitted to executor >> {database_name}.dbo.{table_name}")

                # export_zip_move(database_name, table_name, df, src_tbl_dir, dst_tbl_dir)
                
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


log()
run(batches)