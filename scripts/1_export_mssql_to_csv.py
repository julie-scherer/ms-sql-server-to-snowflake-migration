
## Imports
import os
import concurrent.futures
import pandas as pd
import pyodbc
import time
import logging
import datetime


from utils import Utils
DATABASE1, DATABASE2, DATABASE3, MSSQL_SERVER, OUTPUT_DIR, MAX_WORKERS, TIMEOUT_SECONDS = Utils.DATABASE1, Utils.DATABASE2, Utils.DATABASE3, Utils.MSSQL_SERVER, Utils.OUTPUT_DIR, Utils.MAX_WORKERS, Utils.TIMEOUT_SECONDS

## Microsoft SQL server
server=MSSQL_SERVER

## Path to export results
output_dir=OUTPUT_DIR

## Configurations for parallel processing
max_workers = MAX_WORKERS # defaults to 5 workers if no value in .env
timeout = datetime.timedelta(seconds=TIMEOUT_SECONDS) # defaults to 5 mins if no value in .env

## MSSQL databases
# format: ('database_name', ['column_name', 'column_name', ... ])
database1=DATABASE1
database2=DATABASE2
database3=DATABASE3

def log(database_name):
    log_path = os.path.join(output_dir, 'logs') 
    os.makedirs(log_path, exist_ok=True) 
    log_file = datetime.datetime.now().strftime(f'{database_name}__%Y_%m_%d__%H_%M_%S_%p.txt')
    log_file_path = os.path.join(log_path, log_file)
    try: #! delete
        logging.basicConfig(
            filename=log_file_path,
            format='[%(levelname)s] %(asctime)s:  %(message)s',
            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logging.info(f"TEST MSG") #! delete
        logging.warning(f"TEST MSG") #! delete
        logging.error(f"TEST MSG") #! delete
    except Exception as e: #! delete
        print(f"error: {e}") #! delete

def connect(database_name):
    # connection_string = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database_name}"
    # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database_name}")
    return conn

def make_path(database_name, table_name):
    ## Set the path to where the CSV files will be saved
    table_path = os.path.join(output_dir, database_name, table_name)
    os.makedirs(table_path, exist_ok=True) # create table folder
    return table_path

def read_mssql(table_name, conn):
    try:
        df = pd.read_sql(
            sql=f'''SELECT * FROM [dbo].{table_name}''',
            con=conn
        )
        logging.info(f"Retrieved data for {table_name}: \n{df.head(5)}\n")
    except Exception as e:
        logging.error(f"Unable to retrieve data for {table_name}: {e}")
    return df

def load_csv(df, table_name, table_path):
    try:
        df.to_csv(
            os.path.join(table_path, f"{table_name}_Backfill.csv.gz"), 
            header=False,
            index=False, 
            sep="|", 
            na_rep='NULL',
            compression='gzip',
            doublequote=True,
        )
        logging.info(f"Loaded {table_name} to {table_path}")
    except Exception as e:
        logging.error(f"Unable to load data for {table_name}: {e}")

def write_mssql_to_csv(database_name, table_name):
    conn = connect(database_name)
    table_path = make_path(database_name, table_name)
    df = read_mssql(table_name, conn)
    load_csv(df, table_name, table_path)

def run(databases):
    for database in databases:
        database_name, table_names = database[0], database[1]
        futures = []
        log(database_name)

        logging.info(f"Running {database_name}...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            db_start = time.time()

            for table_name in table_names:
                logging.info(f"Running {table_name}...")
                tbl_start = time.time()
                try:
                    # Submit tasks for each table in each database to the executor
                    logging.info(f"Submitting tasks for {table_name} to executor...")
                    future = executor.submit(write_mssql_to_csv, database_name, table_name)
                    futures.append(future)

                    # Wait for the tasks to complete with a timeout
                    logging.info(f"Waiting for {table_name} tasks to complete with a timeout of {timeout.total_seconds()}.")
                    concurrent.futures.wait(futures, timeout=timeout.total_seconds())

                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                    if result is not None:
                        logging.info(f"Task completed! {result}")
                
                except Exception as e:
                    logging.error(f"Unable to execute concurrent futures task for {table_name}: {e}")
                
                tbl_end = time.time()
                tbl_exec_time = tbl_end - tbl_start
                logging.info(f'**** QUERY DURATION: {table_name} = {tbl_exec_time} ****')
            
            db_end = time.time()
            db_exec_time = db_end - db_start
            logging.info(f'**** QUERY DURATION: {database_name} = {db_exec_time} ****')

            for tbl_idx, future in enumerate(futures):
                if future.done() and not future.cancelled(): # Handle the completion of the task
                    logging.info(f'{tbl_idx} completed successfully.')
                    print(f"[INFO]: {tbl_idx} completed successfully.")
                elif future.done() and future.cancelled():
                    logging.warning(f'{tbl_idx} was cancelled.')
                    print(f"[WARNING]: {tbl_idx} was cancelled.")
                else:
                    logging.error(f'{tbl_idx} did not complete.')
                    print(f"[ERROR]: {tbl_idx} did not complete.")
            
            logging.info(f"{database_name} finished!")

def main():
    print("*******************************************\n"
          f"                 Running...               \n"
          "*******************************************\n\n")
    databases = [database1, database2, database3]
    run(databases)
    print("*******************************************\n"
          f"                 Finished!                \n"
          "*******************************************\n\n")

if __name__ == '__main__':
    main()
