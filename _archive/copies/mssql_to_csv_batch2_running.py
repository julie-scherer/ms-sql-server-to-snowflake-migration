## Imports
import os
import time
import logging
from datetime import datetime,timedelta
import concurrent.futures  # concurrent.futures module for parallel processing
import subprocess
import os
import shutil

## Configurations for parallel processing
max_workers = 5
timeout = timedelta(seconds=90)

from utils import Utils
server = Utils.MSSQL_SERVER  # Name of Microsoft SQL server
batches = Utils.BATCH # MSSQL databases and their tables

local_dir = os.getcwd()
data_dir = os.path.join(local_dir, 'data')
logs_dir = os.path.join(local_dir, 'logs')
dst_dir = r'\\ictfs01\SharedUSA\IT\Batch\DW\BCP'

def make_dir(dir):
    os.makedirs(dir, exist_ok=True)
    return dir

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


def bcp_to_file(database_name, table_name, csv_path, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
    logging.info(f"Running BCP command for {database_name}.dbo.{table_name}... ")
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
        
        bcp_logs = make_dir(os.path.join(logs_dir, 'bcp_run'))
        bcp_path = os.path.join(bcp_logs, datetime.now().strftime(f'BCP_{database_name}_{table_name}_%Y%m%d_%H%M_Log.txt'))
        err_logs = make_dir(os.path.join(logs_dir, 'bcp_errs'))
        err_path = os.path.join(err_logs, datetime.now().strftime(f'BCP_{database_name}_{table_name}_%Y%m%d_%H%M_Error.txt'))
        
        # offset = 1
        # fetch_next = 10000000
        subprocess.run([
            "bcp",
            f"""SELECT * 
                FROM {database_name}.[dbo].[{table_name}]
                -- ORDER BY (
                --     SELECT COLUMN_NAME
                --     FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                --     WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                --     AND TABLE_CATALOG = '{database_name}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
                -- )
                -- OFFSET 10000001 ROWS
                -- FETCH NEXT 10000000 ROWS ONLY
                ;
            """,
            "queryout", csv_path, "-c",
            "-t" + delimiter, "-r" + linebreak,
            "-T", "-S", server,
            "-o", bcp_path,
            "-e", err_path,
        ], stderr=subprocess.PIPE)
        
        logging.info(f"Successfully executed BCP for {database_name}.dbo.{table_name}: {csv_path}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        logging.info(f">> Path: {csv_path}")
        return True
    
    except Exception as e:
        logging.error(f"Error occurred while exporting {database_name}.dbo.{table_name}: {e}")
        if os.path.exists(csv_path):
            os.remove(csv_path)
            logging.info(f"Deleted {csv_path}")
        return False


def pigz_file(csv_path):
    logging.info(f"Running PigZ to compress {csv_path}... ")
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
        logging.error(f"Error occurred while compressing {csv_path}: {e}")
        return False

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

# def bcp_pigz(database_name, table_name, csv_path, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
#     logging.info(f"Running BCP command for {database_name}.dbo.{table_name}... ")
#     try:
#         if os.path.exists(csv_path):
#             os.remove(csv_path)
        
#         bcp_logs = make_dir(os.path.join(logs_dir, 'bcp_run'))
#         bcp_path = os.path.join(bcp_logs, datetime.now().strftime(f'%Y%m%d_%H%M_BCP_{database_name}_{table_name}_Log.txt'))
#         err_logs = make_dir(os.path.join(logs_dir, 'bcp_errs'))
#         err_path = os.path.join(err_logs, datetime.now().strftime(f'%Y%m%d_%H%M_BCP_{database_name}_{table_name}_Error.txt'))
        
#         # offset = 1
#         # fetch_next = 10000000
#         subprocess.run([
#             "bcp",
#             f"""SELECT * 
#                 FROM {database_name}.[dbo].[{table_name}]
#                 -- ORDER BY (
#                 --     SELECT COLUMN_NAME
#                 --     FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
#                 --     WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
#                 --     AND TABLE_CATALOG = '{database_name}' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
#                 -- )
#                 -- OFFSET 10000001 ROWS
#                 -- FETCH NEXT 10000000 ROWS ONLY
#                 ;
#             """,
#             "queryout", csv_path, "-c",
#             "-t" + delimiter, "-r" + linebreak,
#             "-T", "-S", server,
#             "-o", bcp_path,
#             "-e", err_path,
#         ], stderr=subprocess.PIPE)
        
#         logging.info(f"Successfully executed BCP for {database_name}.dbo.{table_name}: {csv_path}")
#         logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
#         logging.info(f">> Path: {csv_path}")
        
#         logging.info(f"Running PigZ command for {csv_path}... ")
#         proc = subprocess.Popen(
#             f'pigz {csv_path}'.split(maxsplit=1),
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE
#         )
#         out, err = proc.communicate()
#         if err != b'':
#             raise ValueError(f'The following error occured while running pigZ {err}')
#         if os.path.exists(csv_path + '.gz') is False:
#             raise ValueError(f"File does not exist: {csv_path+'.gz'}")
        
#         logging.info(f"Successfully executed PigZ command: {csv_path+'.gz'}")
#         return True
    
#     except Exception as e:
#         logging.error(f"Error occurred while exporting {database_name}.dbo.{table_name}: {e}")
#         return False



def mssql_to_csv(database_name, table_name):
    src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name))
    dst_tbl_dir = make_dir(os.path.join(dst_dir, database_name, table_name))
    csv_filename = f"{table_name}_Backfill.csv"
    csv_path = os.path.join(src_tbl_dir, csv_filename)

    bcp_to_file(database_name, table_name, csv_path)
    pigz_file(csv_path)
    move_zip(src_tbl_dir, dst_tbl_dir, csv_filename)

    logging.info(f"Finished {database_name}.dbo.{table_name}")

def run_mssql_to_csv(batches):
    for idx, batch in enumerate(batches):
        database_name, table_names = batch[0], batch[1]
        logging.info(f"Running batch {idx+1} >> {database_name}")
        
        futures = []  # Create an empty array to store "futures" tasks

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

            # Submit tasks for each table in the database to the futures executor
            for idx, table_name in enumerate(table_names):
                future = executor.submit(mssql_to_csv, database_name, table_name)
                futures.append(future)
                logging.info(f"Task {idx+1} submitted to executor >> {database_name}.dbo.{table_name}")

            try:
                concurrent.futures.wait(futures) # Wait for the tasks to complete
                logging.info(f"Executing tasks with a timeout of {timeout.total_seconds()}.")
            except Exception as e:
                logging.error(f"Error occurred while executing concurrent futures task for {table_name}: {e}") # Log an error message if Error occurred while execute concurrent futures task
            
            for future in concurrent.futures.as_completed(futures, timeout=timeout.total_seconds()):
                future.result()
            
            for tbl_idx, future in enumerate(futures): # Handle the completion of the task
                if future.done() and not future.cancelled(): # Handle the completion of the task
                    logging.info(f'Task {tbl_idx+1} completed successfully.')
                elif future.done() and future.cancelled():
                    logging.warning(f'Task {tbl_idx+1} was cancelled.')
                else:
                    logging.error(f'Task {tbl_idx+1} did not complete.')

def main():
    print("*******************************************\n"
          f"                 Running...               \n"
          "*******************************************\n")
    
    log()
    make_dir(logs_dir)
    
    start = time.time()
    start_datetime = datetime.now()
    logging.info(f"Start time: {start_datetime.hour}:{start_datetime.minute}:{start_datetime.second}")
    
    run_mssql_to_csv(batches)
    
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
