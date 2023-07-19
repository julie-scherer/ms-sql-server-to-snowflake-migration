## Imports
import os
import time
import logging
import datetime
import concurrent.futures  # concurrent.futures module for parallel processing
import subprocess
import os

current_dir=os.getcwd() # Get the current working directory

## Import the Utils class from utils.py
from utils import Utils
server = Utils.MSSQL_SERVER  # Name of Microsoft SQL server

batch = Utils.BATCH # MSSQL databases and their tables

project_dir = current_dir  # Folder where the app is run and logs will be exported
csvs_dir = os.path.join(project_dir, 'csvs') # Folder where CSV results should be exported
os.makedirs(csvs_dir, exist_ok=True)  # Create the log directory if it doesn't exist
logs_dir = os.path.join(project_dir, 'logs')  # Create a path for log files in the project directory
os.makedirs(logs_dir, exist_ok=True)  # Create the log directory if it doesn't exist

## Configurations for parallel processing
max_workers = 3
timeout = datetime.timedelta(seconds=90)

## Function to log CLI output
def log():
    log_file_name = datetime.datetime.now().strftime(f'Run_%Y_%m_%d_%H_%M_%S_%p.txt')  # Create a log file name based on the current timestamp and database name
    log_file_path = os.path.join(logs_dir, log_file_name)  # Create the full path for the log file
    logging.basicConfig(
        filename=log_file_path,
        format='[%(levelname)s] %(asctime)s:  %(message)s',
        filemode='w')  # Set up the logging configuration with the log file path
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    print(f"Log file saved at {log_file_path}")

## Create path where the CSV files will be saved
def make_path(database_name, table_name):
    table_path = os.path.join(csvs_dir, database_name, table_name)  # Create the path for the table's CSV file
    os.makedirs(table_path, exist_ok=True)  # Create the table folder if it doesn't exist
    return table_path

## Export CSVs using BCP utility
def bcp_to_file(database_name, table_name, delimiter='|', linebreak=os.linesep, header=False):
    try:
        table_path = make_path(database_name, table_name)  # Generate the file path for the table
        filepath = os.path.join(table_path, f"{table_name}_Backfill.csv.gz")
        subprocess.run([
            "bcp", 
            f"{database_name}.dbo.{table_name}", 
            "out", filepath, 
            "-c", 
            "-t" + delimiter, 
            "-r" + linebreak, 
            "-S", server, 
            "-T",
            "-o", os.path.join(logs_dir, datetime.datetime.now().strftime(f'BCP_{database_name}_{table_name}_%Y%m%d.txt')),
            "-b", "10000"
        ], stderr=subprocess.PIPE)
        if os.path.isfile(filepath) is False:
            raise Exception(f'File not found: {filepath}')
        print(f"Successfully exported {database_name}.dbo.{table_name}! \n\tFile Size: {os.path.getsize(filepath)} bytes \n\tPath: {filepath}")
        logging.info(f"Successfully exported {database_name}.dbo.{table_name}! \n\tFile Size: {os.path.getsize(filepath)} bytes \n\tPath: {filepath}")
    except Exception as e:
        print(f"Unable to export {database_name}.dbo.{table_name} due to error: {e}" )
        logging.error(f"Unable to export {database_name}.dbo.{table_name} due to error: {e}" )
        

# This function compresses a file using the external 'pigz' command-line tool.
# It takes the filepath of the file as input.
def pigz_file(filepath):
    # Execute the 'pigz' command with the filepath as an argument, capturing the output
    proc = subprocess.Popen(f'pigz {filepath}'.split(maxsplit=1), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if err != b'': # If there is any error output, raise a ValueError with the error message
        raise ValueError(f'The following error occurred: {err}')
    print(f'Successfully zipped {filepath}.') 

def run(databases):
    log()  # Create log file and start logging
    for database in databases:
        database_name, table_names = database[0], database[1]  # Extract the database name and table names from tuple
        futures = []  # Create an empty array to store "futures" tasks

        db_start = time.time()  # Start the execution timer
        logging.info(f"\n*** Starting {database_name} ***")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for table_name in table_names:
                # Submit tasks for each table in the database to the futures executor
                future = executor.submit(bcp_to_file, database_name, table_name)
                futures.append(future)
                logging.info(f"Submitted {table_name} task to executor.")
                # print(f"Submitted {table_name} task to executor.")

            try:
                concurrent.futures.wait(futures) # Wait for the tasks to complete
                logging.info(f"Executing tasks with a timeout of {timeout.total_seconds()}.")
                # print(f"Executing tasks with a timeout of {timeout.total_seconds()}.")
            except Exception as e:
                logging.error(f"Unable to execute concurrent futures task for {table_name}: {e}") # Log an error message if unable to execute concurrent futures task
                # print(f"Unable to execute concurrent futures task for {table_name}: {e}") # Log an error message if unable to execute concurrent futures task
            
            for future in concurrent.futures.as_completed(futures, timeout=timeout.total_seconds()):
                future.result()
            
            for tbl_idx, future in enumerate(futures): # Handle the completion of the task
                if future.done() and not future.cancelled(): # Handle the completion of the task
                    logging.info(f'{tbl_idx} completed successfully.')
                    # print(f'{tbl_idx} completed successfully.')
                elif future.done() and future.cancelled():
                    logging.warning(f'{tbl_idx} was cancelled.')
                    # print(f'{tbl_idx} was cancelled.')
                else:
                    logging.error(f'{tbl_idx} did not complete.')
                    # print(f'{tbl_idx} did not complete.')
            
        db_end = time.time() # Log execution end time
        db_exec_time = db_end - db_start # Compute total time taken to execute the tasks
        
        logging.info(f'Total time to execute {database_name} : {db_exec_time}') 
        print(f'Total time to execute {database_name} : {db_exec_time}') 

        logging.info(f"*** {database_name} finished! ***\n")


def main():
    print("*******************************************\n"
          f"                 Running...               \n"
          "*******************************************\n")
    run(batch)
    print("\n*******************************************\n"
          f"                 Finished!                \n"
          "*******************************************\n")


if __name__ == '__main__':
    main()
