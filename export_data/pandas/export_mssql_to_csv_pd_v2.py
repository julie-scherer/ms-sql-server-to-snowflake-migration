## Imports
import os
import time
import logging
import datetime
import concurrent.futures  # concurrent.futures module for parallel processing
import pandas as pd  # pandas library
from sqlalchemy import create_engine, text # modules from sqlalchemy
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import URL
# import pyodbc  #! not in use
# import connectorx as cx  #! not in use


## Import the Utils class from utils.py
from utils import Utils
server = Utils.MSSQL_SERVER  # Name of Microsoft SQL server
project_dir = Utils.PROJECT_DIR  # Folder where the app is run and logs will be exported
output_dir = Utils.OUTPUT_DIR  # Folder where CSV results should be exported
batch = Utils.BATCH # MSSQL databases and their tables

## Configurations for parallel processing
max_workers = 7
timeout = datetime.timedelta(seconds=90)


## Function to log CLI output
def log():
    log_path = os.path.join(project_dir, 'logs')  # Create a path for log files in the project directory
    os.makedirs(log_path, exist_ok=True)  # Create the log directory if it doesn't exist
    log_file_name = datetime.datetime.now().strftime(f'Run_%Y_%m_%d_%H_%M_%S_%p.txt')  # Create a log file name based on the current timestamp and database name
    log_file_path = os.path.join(log_path, log_file_name)  # Create the full path for the log file
    logging.basicConfig(
        filename=log_file_path,
        format='[%(levelname)s] %(asctime)s:  %(message)s',
        filemode='w')  # Set up the logging configuration with the log file path
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    print(f"Log file saved at {log_file_path}")


## Connect to MSSQL database using SQLAlchemy
def connect(database_name):
    connection_string = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database_name}"  # Define the connection string for the MSSQL server
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})  # Create the connection URL using the connection string
    engine = create_engine(connection_url, poolclass=QueuePool)  # Create a SQLAlchemy engine with the connection URL and QueuePool
    # cursor = pyodbc.connect(connection_string).cursor() #! not using pyodbc
    return engine  # Return the created engine

## Create path where the CSV files will be saved
def make_path(database_name, table_name):
    table_path = os.path.join(output_dir, database_name, table_name)  # Create the path for the table's CSV file
    os.makedirs(table_path, exist_ok=True)  # Create the table folder if it doesn't exist
    return table_path

## Tasks to execute in parallel
def write_mssql_to_csv(database_name, table_name, engine):
    try:
        with engine.connect() as sqlalc_conn:
            df = pd.read_sql_query(
                text(f'''SELECT * FROM [dbo].[{table_name}]'''),  # Select all columns from the specified table
                sqlalc_conn
            )
            logging.info(f"\nRead from {table_name} table successful! Table shape: {df.shape} \n{df.head(5)}") # Log the first 5 rows of the DataFrame if it was a success
            # print(f"\nRead from {table_name} table successful! Table shape: {df.shape} \n{df.head(5)}") # Log the first 5 rows of the DataFrame if it was a success
        engine.dispose()  # Release the database connection to free up resources
        table_path = make_path(database_name, table_name)  # Generate the file path for the table
    
    except Exception as e:
        logging.error(f"\n[ERROR] - {table_name} - Unable to retrieve data for {table_name} due to {e}") # Log an error message if data retrieval fails
        # print(f"\n[ERROR] - {table_name} - Unable to retrieve data for {table_name} due to {e}") # Log an error message if data retrieval fails

    try:
        df.to_csv(
            os.path.join(table_path, f"{table_name}_Backfill.csv.gz"),  # Specify the output file path and name
            header=False,  # Exclude the column headers from the CSV
            index=False,  # Exclude the row index from the CSV
            sep="|",  # Use the pipe symbol as the column separator
            na_rep='NULL',  # Replace missing values with 'NULL'
            compression='gzip',  # Compress the CSV file using gzip
            doublequote=True,  # Enable double quoting for values
        )
        logging.info(f"\nWrite to {table_name}.csv.gz successful! Saved at {table_path}") # Log a success message if CSV file writing is successful
        # print(f"\nWrite to {table_name}.csv.gz successful! Saved at {table_path}") # Log a success message if CSV file writing is successful
    
    except Exception as e:
        logging.error(f"\n[ERROR] - {table_name} - Unable to write csv file for {table_name} due to {e}") # Log an error message if CSV file writing fails
        # print(f"\n[ERROR] - {table_name} - Unable to write csv file for {table_name} due to {e}") # Log an error message if CSV file writing fails


def run(databases):
    log()  # Create log file and start logging

    for database in databases:
        database_name, table_names = database[0], database[1]  # Extract the database name and table names from tuple
        engine = connect(database_name)  # Establish a connection to the database
        futures = []  # Create an empty array to store "futures" tasks

        db_start = time.time()  # Start the execution timer
        logging.info(f"\n*** Starting {database_name} ***")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for table_name in table_names:
                # Submit tasks for each table in the database to the futures executor
                future = executor.submit(write_mssql_to_csv, database_name, table_name, engine)
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
        # print(f'Total time to execute {database_name} : {db_exec_time}') 

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