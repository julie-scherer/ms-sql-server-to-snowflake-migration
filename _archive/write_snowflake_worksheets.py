import os
import openpyxl
import re

from utils import create_tbl_syntax, copy_into_tbl_syntax, Utils

## Get the current working directory
current_dir=os.getcwd()

## MSSQL databases ~ See .env for format >> ('database_name', ['column_name', 'column_name', ... ])
database1=Utils.DATABASE1
database2=Utils.DATABASE2
database3=Utils.DATABASE3
databases = [database1] + [database2] + [database3]

create_tables = 'create_tables'
copy_into_tables = 'copy_into_tables'

## Defaults - DO NOT CHANGE
def _format_schema(schema: str):
    schema = re.sub(r'(?<![0-9]),', ',\n\t', schema)
    schema = schema.replace('"en-ci"',"'en-ci'")
    return schema

def custom_replacements(schema: str , replacements: list[tuple]):
    ## Custom changes
    for replacement in replacements:
        schema = schema.replace(replacement[0], replacement[1])
    return schema

def format_cols_for_copy_into(table_name: str, fschema: str):
    data_types = [] # create an empty array to store data types
    schema_text = '' # create an empty array to save formatted schema

    for t in fschema.split(',\n\t'): # input `fschema` is the same schema used in the create tables query
        table_data = t.split(' ')
        data_types.append((table_data[0], table_data[1]))
        not_nullable = True if 'NOT' in table_data else False
    
    start, end = 1, len(data_types)
    for tbl, dt in data_types:
        cast = re.sub(r'[\d|\$|\(|\)]','', dt)
        ext = f" -- ${start}: {tbl} {dt} {'NOT NULL' if not_nullable else 'NULL'} \n\t\t" if start < end else ''
        if cast == 'TIMESTAMP_LTZ':
            schema_text += f"to_timestamp_ntz(${start})," + ext
        else:
            schema_text += f"(${start})::{cast.lower()}," + ext
        start += 1
    schema_text = schema_text[:-1] + f" -- ${start}: {tbl} {dt} {'NULL' if not_nullable else 'NOT NULL'}" 
    return schema_text


def export_sql_script(query, table_name):
    # Specify the file path and name for the SQL file
    sql_file_path = f"{current_dir}/sfsql/{table_name}.sql"
    # Create the SQL file and write the SQL query
    with open(sql_file_path, 'w') as sql_file:
        sql_file.write(query)

def write_snowflake_queries():
    schemas_found = {}
    create_tbl_text = ''
    copy_into_text = ''
    for database in databases:
        database_name, table_names = database[0], database[1]
        
        # Load the Excel file for the database
        workbook = openpyxl.load_workbook(f"{current_dir}/data/{database_name}.xlsx")
        worksheet = workbook['sheet1'] # Access a specific worksheet by name

        for table_name in table_names:
            # Read data from the worksheet
            for row in worksheet.iter_rows(min_row=2, values_only=True):
                ## Get the table name and table schema (columns, data types, etc.)
                table_name, raw_schema = row[0], row[1]
                
                ## Skip the table if it's already been defined and the schema is the same
                if table_name in schemas_found.keys() and raw_schema == schemas_found.get(table_name):
                    continue # continue tells the program to return back to the beginning of the for loop
                ## Otherwise, finish the block...

                ## Add the table's raw schema to the table definitions dictionary
                schemas_found[table_name] = raw_schema

                ## Format the schema
                fschema = _format_schema(raw_schema) ## do not delete or modify this function
                # print(f"Formatted schema: {fschema}\n")

                # * CREATE TABLES
                # replacement expects a list of tuples
                # first value is the string to replace and second value is the string to replace WITH
                # -> for example [ ('FILENAME', 'FILENAME_') ] changes 'FILENAME' to 'FILENAME_'
                replacements = [ ("FILENAME", "FILENAME_") ] 
                cstcols = custom_replacements(fschema, replacements) # if there are no replacements, it will simply return fschema
                ctsql = create_tbl_syntax(table_name, cstcols) # get create table syntax and insert table data
                create_tbl_text += ctsql # add this table's sql query to the "create table text" with the rest of the tables queries
                
                # * COPY INTO
                fmtcols = format_cols_for_copy_into(table_name, fschema) # get formatted columns
                cpsql = copy_into_tbl_syntax(table_name, fmtcols) # get copy into table syntax and insert table data
                copy_into_text += cpsql # add this table's sql query to the "create table text" with the rest of the tables queries

            # Close the workbook
            workbook.close()
        
        export_sql_script(create_tbl_text, create_tables)
        export_sql_script(copy_into_text, copy_into_tables)
        
    print(f"Number of table definitions: {len(schemas_found)}")
        


if __name__ == '__main__':
    write_snowflake_queries()
