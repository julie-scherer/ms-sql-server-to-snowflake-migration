import os
import pandas as pd
import re
from imports.ddl_utils import Utils

## COPY INTO parameters
batch = Utils.BATCH # List of databases and tables to run in batch (see utils.py)

tbl_start_idx = Utils.COUNT_TABLES_FINISHED + 1 # What table number/index are you starting at? Add 1 to the number of tables generated in last batch(es)
print(f"Starting at {tbl_start_idx}")

current_dir = os.getcwd() # Get the current working directory
sf_database = Utils.SF_DATABASE # Name of the Snowflake database you're copying into
source_data = f"{current_dir}/data/{sf_database}.csv" # Where to find ddl csv
aod = '2023-07-26' # asOfDate

file_format = \
"""TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\\n'
    SKIP_HEADER = 0
    EMPTY_FIELD_AS_NULL = TRUE """
# file_format = 'FORMAT NAME = DEV_JS.STG.CREO_B3_BCP_CSV_ZIP_CRT_LNBRK_SH0' # file format to use
# file_format = 'FORMAT NAME = STG.LD_CSV_PIPE_SH1_EON_GZ' # file format to use
# file_format = 'FORMAT NAME = STG.SRC_CSV_PIPE_SH1_EON_GZ' # file format to use

pattern_suffix = 'Backfill_[0-9]+\.csv\.gz'

# Set the testing mode flag to True or False
if os.getenv('TESTING') == 'True':
    testing = True 
else: 
    testing = False

print(f"Testing? {testing}\n")
if testing:
    output_filename = f'{sf_database.upper()}_COPY_INTO_DEV' # Name of the SQL file that will be created in `sfsql/` subfolder
else:
    output_filename = f'{sf_database.upper()}_COPY_INTO_PROD' # Name of the SQL file that will be created in `sfsql/` subfolder

## Text you want to replace from the MSSQL output
# Expects a list of tuples with the first string in the tuple the string to replace, and the second string as the string to replace WITH
#   >> for example [ ('FILENAME', 'FILENAME_') ] changes 'FILENAME' to 'FILENAME_'
replacements = [('','')]

# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

## COPY INTO query generated for each table
def copy_into_tbl_query(idx, sf_database, mssql_database, table_name, columns, aod, file_format, col_names_joined, index, testing=False):
    dev_wh = 'ARES' + '.' if testing else '' # use ARES or the name of your DEV_* database
    prod_wh = 'ZEUS' + '.' if testing else '' # production warehouse (where file format is stored)
    tests = get_tests(dev_wh, mssql_database, table_name, col_names_joined, file_format, index) if testing else ''    
    return f"""
-- // TABLE {idx}: {table_name}
COPY INTO {dev_wh}STG.{sf_database}_{table_name}_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{aod}'), 
        {columns}
    FROM @ETL.INBOUND/{sf_database}/Backfill/{table_name}/
)
FILE_FORMAT = (
    {file_format}
)
PATTERN = '.*{table_name}_{pattern_suffix}';
""" + tests

## Additional lines added to query if in testing mode
def get_tests(dev_wh, database_name, table_name, col_names_joined, file_format, index):
    return f"""/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.{database_name}_{table_name.upper()}_HIST; -- drop records
LIST @ETL.INBOUND/{sf_database}/Backfill/{table_name}/; -- list files in S3
SELECT {dev_wh}ETL.COPYSELECT('STG','{database_name}_{table_name}_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM {dev_wh}STG.{database_name}_{table_name}_HIST; -- check row count
SELECT TOP 10 * FROM {dev_wh}STG.{database_name}_{table_name}_HIST; -- preview data
*/

"""

# -- // PREVIEW STAGED DATA IN S3 --
# SELECT 
#     -- TOP 10
#     -- METADATA$FILE_ROW_NUMBER
#     METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
#     < insert $cols >
#     {col_names_joined}
# FROM @ETL.INBOUND/{sf_database}/Backfill/{table_name}/
# (
#     FILE_FORMAT => '{file_format}',
#     PATTERN => '.*{table_name}_{pattern_suffix}'
# );

## Format columns for COPY INTO query
def format_cols_for_copy_into(columns):
    formatted_columns = []
    for index, column in enumerate(columns, start=1):
        column_data = column.strip().split(' ') # remove any trailing white spaces on the left or right ends of the string, and then split the string into a list
        col_name, col_type = column_data[0], column_data[1]

        cast = re.sub(r'[^a-zA-Z_]', '', col_type).lower() # remove any characters that are not a letter or underscore, and turns to lowercase
        cmt = f"\t-- ${index}: {col_name} {col_type} {'NOT NULL' if 'NOT' in column_data else 'NULL'}" # comment to add at end of line
        
        formatted_col = f"(${index})::{cast}" if cast != 'timestamp_ltz' else f"to_timestamp_ntz(${index})" # cast the column to the correct data type
        formatted_col += f', {cmt}' if (index < len(columns)) else f' {cmt}' # add comment with a preceeding comma, except if its the last column, then don't add a comma

        formatted_columns.append(formatted_col) # Append the single formatted column to the list

    return '\n\t\t'.join(formatted_columns)  # Join multiple formatted columns in the list with a newline and 2 tabs for formatting

## Export SQL file
def export_sql_script(query, output_filename):
    sql_file_path = f"{current_dir}/sfsql/{output_filename}.sql" # Specify the file path and name for the SQL file
    with open(sql_file_path, 'w') as sql_file: # Create the SQL file and write the SQL query
        sql_file.write(query)
    print(f"SQL file saved at {sql_file_path}")

## Generate Snowflake script
def write_copy_into_snowflake():
    schemas_found = {} # Dictionary to store table names and schemas to avoid duplicates
    copy_into_text = '' # Text to store all the COPY INTO queries
    table_idx = tbl_start_idx # Set current table index to the start index defined above

    for idx,db in enumerate(batch):
        mssql_database, mssql_table_names = db[0], db[1]
        
        # Read the CSV file into a Pandas DataFrame
        try:
            df = pd.read_csv(source_data)
            # print(f"Retrieved source table: \n{df.head(5)}\n")
        except Exception as e:
            print(f"[ERROR] Unable to retrieve data from {source_data}")
        
        print(f"Running batch #{idx+1}... \nInput: {db}\n")
        for index, row in df.iterrows():
            table_name = row['TableName'] if row.get('TableName') else row[0]
            raw_ddl = row['ColumnData'] if row.get('ColumnData') else row[1]
            # print(f"\n{table_idx}. {table_name}: {raw_ddl.replace(' ,',', ')}")

            # Skip tables not in the batch
            if table_name not in mssql_table_names:
                continue
            # Skip duplicate tables
            if (f"{mssql_database}.{table_name}" in schemas_found.keys()) and (raw_ddl == schemas_found.get(f"{mssql_database}.{table_name}")):
                continue # return back to the beginning of the for loop
            # Add <database_name>.<table_name> and corresponding DDL to schemas found
            schemas_found[f"{mssql_database}.{table_name}"] = raw_ddl

            # Split the string of columns and data types into a list
            columns = re.split(r'(?<![0-9]),', raw_ddl)
            column_names = [column.split(' ')[0] for column in columns]
            # print(f"Column names: \n{column_names}")

            col_names_joined = ', '.join(column_names)
            # print(f"Column names joined: \n{col_names_joined}")

            # Format the schema
            fschema = re.sub(r'(?<![0-9]),', ',\n\t', raw_ddl).replace('"en-ci"',"'en-ci'").replace(' ,',',')

            # Replace any strings if specified above
            if replacements and replacements != [('','')]:
                print("Replacements found. Working on it...")
                for replacement in replacements:
                    fschema = fschema.replace(replacement[0], replacement[1])

            # Get the COPY INTO query for the current table and add to longer text with all the queries
            fcols = format_cols_for_copy_into(columns) # get formatted columns
            copy_into_sql = copy_into_tbl_query(table_idx, sf_database, mssql_database, table_name, fcols, aod, file_format, col_names_joined, index+1, testing,) # get copy into table syntax and insert table data
            copy_into_text += copy_into_sql # add this table's sql query to the copy into text with the rest of the tables queries

            # print(f"{table_idx}. {mssql_database}.{table_name} finished!")
            # print(f"{copy_into_sql}")

            # Add 1 to the current table index
            table_idx += 1
    
    # Log the number of queries generated
    print(f"Done! Number of queries created: {table_idx - tbl_start_idx}\n")

    # Write the final text with all the queries into a SQL file
    export_sql_script(copy_into_text, output_filename)

if __name__ == '__main__':
    write_copy_into_snowflake()
