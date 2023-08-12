## Imports
import os
import time
import logging
import datetime
import subprocess
import os
## Import the Utils class from utils.py
from utils import Utils
server = Utils.MSSQL_SERVER  # Name of Microsoft SQL server
batches = Utils.BATCH # MSSQL databases and their tables

## Configurations for parallel processing
max_workers = 2
timeout = datetime.timedelta(seconds=90)

dst_dir = r'\\ictfs01\SharedUSA\IT\Batch\DW\BCP'

## Function to create directories
def make_dir(dir):
    os.makedirs(dir, exist_ok=True)
    return dir

local_dir = os.getcwd()  # Folder where the app is run
data_dir = os.path.join(local_dir, 'data') # Folder where CSV results should be exported locally
logs_dir = os.path.join(local_dir, 'logs')  # Create a path for log files in the project directory


## Function to log CLI output
def log():
    out_dir = make_dir(os.path.join(logs_dir, 'runs')) # Create the full path for the log file
    log_file_name = datetime.datetime.now().strftime(f'Run_%Y%m%d_%H%M.txt')  # Create a log file name based on the current timestamp and database name
    log_file_path = os.path.join(out_dir, log_file_name) # Create the full path for the log file
    logging.basicConfig(
        filename=log_file_path,
        format='[%(levelname)s] %(asctime)s:  %(message)s',
        filemode='w')  # Set up the logging configuration with the log file path
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    print(f"Log file saved at {log_file_path}")
        
## Export CSVs using BCP utility
def bcp_to_file(database_name, table_name, csv_path, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
    try:
        # if there's another csv file, delete it
        if os.path.exists(csv_path):
            os.remove(csv_path)
        bcp_logs = make_dir(os.path.join(logs_dir, 'bcp_run'))
        bcp_path = os.path.join(bcp_logs, datetime.datetime.now().strftime(f'%Y%m%d_%H%M_BCP_{database_name}_{table_name}_Log.txt'))
        err_logs = make_dir(os.path.join(logs_dir, 'bcp_errs'))
        err_path = os.path.join(err_logs, datetime.datetime.now().strftime(f'%Y%m%d_%H%M_BCP_{database_name}_{table_name}_Error.txt'))
        subprocess.run([
            "bcp", 
            f"SELECT * FROM {database_name}.dbo.{table_name}", 
            "queryout", csv_path, "-c", 
            "-t" + delimiter, "-r" + linebreak, 
            "-T", "-S", server, 
            "-o", bcp_path, 
            "-e", err_path,
            # "-b", str(batch_size),
        ], stderr=subprocess.PIPE)

        logging.info(f"Successfully executed BCP >> {database_name}.dbo.{table_name}")
        logging.info(f">> File Size: {os.path.getsize(csv_path)} bytes")
        logging.info(f">> Path: {csv_path}")
        return 'success'
    
    except Exception as e:
        logging.error(f"Error occurred while exporting {database_name}.dbo.{table_name}: {e}" )
        ## if an error occurred, we want to delete any csv file that mightve been created
        os.remove(csv_path) if os.path.exists(csv_path) else None
        logging.info(f"Deleted {csv_path}")
        return None


# Compress CSV file using the external 'pigz' command-line tool
def pigz_file(csv_path):
    try:
        ## if another zip file exists in source dir, delete file
        if os.path.exists(csv_path + '.gz'):
            os.remove(csv_path + '.gz')
        # Execute the 'pigz' command with the csv path as input, and capture the output
        subprocess.Popen(
            f'pigz {csv_path}'.split(maxsplit=1), 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        ).wait()
        
        # Check if the zip file exists
        if os.path.exists(csv_path + '.gz') is False:
            raise ValueError(f"File does not exist: {csv_path+'.gz'}")
        
        logging.info(f"Successfully zipped csv file: {csv_path+'.gz'}")
        return 'success'
    
    except Exception as e:
        logging.error(f"Error occurred while compressing csv file: {e}")
        return None


## Move zipped CSV to shared drive 
def move_zip(src_tbl_dir, dst_tbl_dir, csv_file):
    src_zip = os.path.join(src_tbl_dir, csv_file + ".gz")
    dst_zip = os.path.join(dst_tbl_dir, csv_file + ".gz")
    try:
        ## if zip file exists in destination dir, delete zip file
        if os.path.exists(dst_zip):
            subprocess.call(f'del {dst_zip}', shell=True)
            logging.info(f"Deleted {dst_zip}")
        
        ## if zip file exists in source dir but not dst dir, move zip file to shared drive
        if os.path.exists(src_zip) and not os.path.exists(dst_zip):
            subprocess.call(f'move {src_zip} {dst_zip}', shell=True)
            logging.info(f"Successfully moved {src_zip} to {dst_zip}")
        
        ## check if the zip file was moved over properly, i.e., both zip files exist, delete zip file in source dir
        if os.path.exists(src_zip) and os.path.exists(dst_zip):
            logging.info(f"Successfully moved zip file to shared drive: {dst_zip}")
            logging.info(f">> File Size: {os.path.getsize(dst_zip)} bytes")
            logging.info(f">> Path: {dst_zip}")

            subprocess.call(f'rm -r {src_tbl_dir}', shell=True)
            logging.info(f"Deleted {src_zip}")

        return 'success'
    
    except Exception as e:
        logging.error(f"Error occurred while moving zip file: {e}")
        return None


## BCP > PigZ > Move zip
def pipeline(database_name, table_name):
    # Create the path for the CSV files
    # make_dir(data_dir)
    src_tbl_dir = make_dir(os.path.join(data_dir, database_name, table_name)) 
    dst_tbl_dir = make_dir(os.path.join(dst_dir, database_name, table_name))
    
    csv_filename = f"{table_name}_Backfill.csv"
    csv_path = os.path.join(src_tbl_dir, csv_filename)
    
    logging.info(f"Running BCP >> {database_name}.dbo.{table_name}")
    # bcp_task = bcp_to_file(database_name, table_name, csv_path)
    # pigz_task = pigz_file(csv_path)
    # move_zip_task = move_zip(src_tbl_dir, dst_tbl_dir, csv_filename)
    # bcp_task >> pigz_task >> move_zip_task
    
    bcp_run = bcp_to_file(database_name, table_name, csv_path)
    logging.info(f"Running BCP >> {database_name}.dbo.{table_name}")
    if bcp_run == 'success':
        logging.info(f"Zipping csv file >> {database_name}.dbo.{table_name}")
        pig_zip = pigz_file(csv_path)
    
        if pig_zip == 'success':
            logging.info(f"Successfully compressed csv file for for {database_name}.dbo.{table_name}")
            logging.info(f"Moving zip and cleaning up resources >> {database_name}.dbo.{table_name}")
            dst_zip = move_zip(src_tbl_dir, dst_tbl_dir, csv_filename)
    
            if dst_zip == 'success':
                logging.info(f"Successfully moved {database_name}.dbo.{table_name} to shared drive")

    # logging.info(f"Finished {database_name}.dbo.{table_name}!")


def run(batches):
    db_start = time.time()  # Start the execution timer
    start_datetime = datetime.datetime.now()
    logging.info(f"Start time: {start_datetime.hour}:{start_datetime.minute}:{start_datetime.second}")
    
    for idx, batch in enumerate(batches):
        database_name, table_names = batch[0], batch[1]  # Extract the database name and table names from tuple
        logging.info(f"Running batch {idx+1} >> {database_name}")

        # Run tasks for each table in the database
        for idx, table_name in enumerate(table_names):
            logging.info(f"Running task {idx+1} >> {database_name}.dbo.{table_name}")
            pipeline(database_name, table_name)
            logging.info(f"Finished task {idx+1} >> {database_name}.dbo.{table_name}")
            
    db_end = time.time() # Log execution end time
    end_datetime = datetime.datetime.now()
    db_exec_time = db_end - db_start # Compute total time taken to execute the tasks
    logging.info(f"End time: {end_datetime.hour}:{end_datetime.minute}:{end_datetime.second}")
    
    logging.info(f'Total time to execute batch: {round(db_exec_time,2)} seconds | {db_exec_time//60} minutes') 
    print(f'Total time to execute batch: {round(db_exec_time,2)} seconds | {db_exec_time//60} minutes') 


def main():
    print("*******************************************\n"
          f"                 Running...               \n"
          "*******************************************\n")
    make_dir(logs_dir)
    log()  # Create log file and start logging
    run(batches)
    print("\n*******************************************\n"
          f"                 Finished!                \n"
          "*******************************************\n")


if __name__ == '__main__':
    main()
