import os
import pandas as pd
import re
from ddl_utils import Utils

## CREATE TABLE parameters

batch_start_index = Utils.COUNT_TABLES_FINISHED + 1 # What table number/index are you starting at? Add 1 to the number of tables generated in last batch(es)
print(f"Starting at {batch_start_index}")

current_dir = os.getcwd() # Get the current working directory
sf_database = Utils.SF_DATABASE # Name of the Snowflake database you're writing DDLs to
source_data = f"{current_dir}/data/{sf_database}.csv" # Where to find ddl csv file
batch = Utils.BATCH # List of databases and tables to run in batch (see utils.py)

# Set the testing mode flag to True or False
if os.getenv('TESTING') == 'True':
    testing = True 
else: 
    testing = False

## Text you want to replace from the MSSQL output
# Expects a list of tuples with the first string in the tuple the string to replace, and the second string as the string to replace WITH
#   >> for example [ ('FILENAME', 'FILENAME_') ] changes 'FILENAME' to 'FILENAME_'
replacements = [('','')]

# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

## CREATE TABLE query generated for each table
def create_tbl_query(idx, database_name, table_name, schema):
    drop_tbl = f"\n-- DROP TABLE IF EXISTS STG.{database_name}_{table_name.upper()}_HIST;" if testing else ''
    truncate_tbl = f"\n-- LIST @etl.inbound/{database_name}/Backfill; STG.{database_name}_{table_name.upper()}_HIST" if testing else ''
    return f"""
-- // TABLE {idx}: {table_name} {drop_tbl}
CREATE TABLE IF NOT EXISTS STG.{database_name}_{table_name.upper()}_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    {schema}
);
"""

## Additional lines added to beginning if SQL if NOT in testing mode
def get_header():
    return """USE SCHEMA {{ SF_DATABASE }}.STG;

/*************************************************************************/
/* Note:																 */ 
/* 1. MUST USE 'CREATE TABLE IF NOT EXISTS'                              */
/* 2. DO NOT USE MASKING POLICIES - They will be applied separately      */
/*************************************************************************/
"""

# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 


## Export SQL file
def export_sql_script(query, table_name):
    sql_file_path = f"{current_dir}/sfsql/{table_name}.sql" # Specify the file path and name for the SQL file
    with open(sql_file_path, 'w') as sql_file: # Create the SQL file and write the SQL query
        sql_file.write(query)
    print(f"SQL file saved at {sql_file_path}")

## Generate Snowflake script
def write_snowflake_ddl():
    schemas_found = {}
    create_tbl_text = get_header() if not testing else ''
    table_idx = batch_start_index

    for idx,db in enumerate(batch):
        batch_table_names = db[1] # second element in batch is list of column names
        
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
            if table_name not in batch_table_names:
                continue
            # Skip duplicate tables
            if table_name in schemas_found.keys() and raw_ddl == schemas_found.get(table_name):
                continue
            # Add the table and raw DDL to schemas found
            schemas_found[table_name] = raw_ddl

            # Format the schema
            fschema = re.sub(r'(?<![0-9]),', ',\n\t', raw_ddl).replace('"en-ci"',"'en-ci'").replace(' ,',',')
            # print(f"{table_idx}. {table_name}: {fschema}")

            # Replace any strings if specified above
            if replacements and replacements != [('','')]:
                print("Replacements found. Working on it...")
                for replacement in replacements:
                    fschema = fschema.replace(replacement[0], replacement[1])
            
            # Get the CREATE query for the current table and add to longer text with all the queries
            create_tbl_sql = create_tbl_query(table_idx, sf_database, table_name, fschema)
            create_tbl_text += create_tbl_sql

            # print(f"{table_idx}. {table_name} finished!")
            # print(f"{create_tbl_sql}")
            
            # Add 1 to the current table index
            table_idx += 1
        
        # Log the number of queries generated
        print(f"Done! Number of queries created: {table_idx - batch_start_index}\n")

        print(f"Testing: {testing}\n")
        if testing:
            output_filename = f'{sf_database.upper()}_DEV_CREATE_TABLES' # Name of the SQL file that will be created in `sfsql/` subfolder
        else:
            output_filename = f'{sf_database.upper()}_PROD_CREATE_TABLES' # Name of the SQL file that will be created in `sfsql/` subfolder

        # Write the final text with all the queries into a SQL file
        export_sql_script(create_tbl_text, output_filename)


if __name__ == '__main__':
    write_snowflake_ddl()
