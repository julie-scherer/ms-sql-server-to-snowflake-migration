import os
import concurrent.futures  # concurrent.futures module for parallel processing
import subprocess
import logging
import pyodbc # connect to mssql via pyodbc driver to get row count


batch_size = 100000
server = 'my_server'
database_name = 'CREO'


# create pyodbc connection for the server & database
conn = pyodbc.connect('Driver={SQL Server};'
                      f'Server={server};'
                      f'Database={database_name};'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()


## Export CSVs using BCP utility
def bcp_export(database_name, table_name, csv_path, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
    # if there's another csv file, delete it
    if os.path.exists(csv_path):
        os.remove(csv_path)
    
    # create logs and error files
    bcp_logs = make_dir(os.path.join(logs_dir, 'bcp_run'))
    bcp_path = os.path.join(bcp_logs, datetime.datetime.now().strftime(f'BCP_{database_name}_{table_name}_%Y%m%d_%H%M_Log.txt'))
    err_logs = make_dir(os.path.join(logs_dir, 'bcp_errs'))
    err_path = os.path.join(err_logs, datetime.datetime.now().strftime(f'BCP_{database_name}_{table_name}_%Y%m%d_%H%M_Error.txt'))
    
    try:
        # run bcp utility
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
        return True
    
    except Exception as e:
        logging.error(f"Error occurred while exporting {database_name}.dbo.{table_name}: {e}" )
        return False

## Export CSVs in batches
def batch_bcp_export(database_name, table_name, csv_path, num_batches=1, delimiter='|', linebreak=os.linesep, batch_size=10000, header=False):
    offset_row_count = 0
    fetch_row_count = batch_size

    # loop through batches
    for batch in num_batches:
        # create logs file
        bcp_logs = make_dir(os.path.join(logs_dir, 'bcp_run'))
        bcp_path = os.path.join(bcp_logs, datetime.datetime.now().strftime(f'Batch_{batch}_BCP_{database_name}_{table_name}_%Y%m%d_%H%M_Log.txt'))
        
        # create error log file
        err_logs = make_dir(os.path.join(logs_dir, 'bcp_errs'))
        err_path = os.path.join(err_logs, datetime.datetime.now().strftime(f'Batch_{batch}_BCP_{database_name}_{table_name}_%Y%m%d_%H%M_Error.txt'))

        try:
            # bcp query using OFFSET and FETCH to skip the first N rows and select the next N rows
            bcp_query = f"""
            SELECT * 
            FROM {database_name}.dbo.{table_name}
            ORDER BY column_list [ASC |DESC]
            OFFSET {offset_row_count} ROWS
            FETCH ROWS {fetch_row_count} ROWS ONLY
            """
            offset_row_count += batch_size + 1
            fetch_row_count += batch_size
        
            # run bcp query
            subprocess.run([
                "bcp", 
                bcp_query, 
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
    
        except Exception as e:
            logging.error(f"Error occurred while exporting batch {batch} for {database_name}.dbo.{table_name}: {e}" )
            return False


def bcp_to_csv(database_name, table_name,):
    # get row count for table
    row_count = cursor.execute(f'SELECT COUNT(*) FROM {database_name}.dbo.{table_name}')
    print(row_count)
    
    # calculate the number of batches by dividing the row count by 100,000
    num_batches = row_count // {batch_size}

    # run BCP
    # if there's only 1 batch, run bcp_to_csv function
    if num_batches <= 1:
        bcp_export(database_name, table_name, csv_path)
        return # exit the function when done
    
    # otherwise, if there are multiple batches, run bcp export tasks in parallel
    batch_futures = []
    batch_workers = 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_workers) as executor:
        # Submit tasks for each table in the database to the futures executor
        for idx, table_name in enumerate(table_names):
            future = executor.submit(batch_bcp_export, database_name, table_name, csv_path, num_batches)
            batch_futures.append(future)
        
        try:
            concurrent.futures.wait(batch_futures) # Wait for the tasks to complete
            logging.info(f"Executing tasks with a timeout of {timeout.total_seconds()}.")
        except Exception as e:
            logging.error(f"Error occurred while executing concurrent futures task for {table_name}: {e}") # Log an error message if Error occurred while execute concurrent futures task
        
        for batch_future in concurrent.futures.as_completed(batch_futures, timeout=timeout.total_seconds()):
            batch_future.result()
        
        for tbl_idx, batch_future in enumerate(batch_futures): # Handle the completion of the task
            if batch_future.done() and not future.cancelled(): # Handle the completion of the task
                logging.info(f'Batch {tbl_idx+1} completed successfully.')
            elif batch_future.done() and batch_future.cancelled():
                logging.warning(f'Batch {tbl_idx+1} was cancelled.')
            else:
                logging.error(f'Batch {tbl_idx+1} did not complete.')




