# SQL Server to Snowflake Data Definition and Loading Migration ✨

This repository contains scripts and instructions to automate many of the steps involved in the migration of data from Microsoft SQL Server to Snowflake data warehouse. The process involves exporting data from SQL Server, converting table definitions to Snowflake syntax, and loading data into Snowflake. 🚀

## **Migration Steps 📝**

1. **Export table definitions from MSSQL**
    * Connect to the MSSQL database
    * Open the **`mssql/get_tbl_definitions.sql`** file 
    * Change **`USE database;`** at the top of the script for your use case
    * Execute the file and check the query result
        * You should see the table names and table definitions with MSSQL data types changed to Snowflake
    * Export the query result as an Excel file and save the Excel file in the **`data/`** folder with the same name as your database

2. **Set up your local environment**
    * Create a virtual environment and install packages:
        ```bash
        cd mssql-snowflake-migration 
        python -m venv .venv  # for Windows
        # python3 -m venv .venv  # for macOS
        source .venv/bin/activate

        pip install --upgrade pip -r requirements.txt --no-cache
        ```
    * Update the Utils values in **`scripts/utils.py`** for your use case (e.g., `MSSQL_SERVER`, `PROJECT_DIR`, `OUTPUT_DIR`, `DATABASE1`, `DATABASE2`, ...)
        * If you don't have multiple databases, just set a value for `DATABASE1` and remove the others
        * When defining `DATABASE1...N`, it expects the first value to be the name of the database in string format and the second value to be a list of the table names, with each table name also in string format

3. **Write Snowflake DDLs and `COPY INTO` scripts**
    * Open **`scripts/write_snowflake_worksheets.py`** and execute to generate the **`CREATE TABLE`** and **`COPY INTO`** queries
        > Note: You may need to modify the code slightly to achieve the desired format and results, particularly for the **`COPY INTO`** script
        >
        * While troubleshooting, you can comment out either of the lines below to create only the **`CREATE TABLE`** script or the **`COPY INTO`** script
            ```python
            export_sql_script(create_tbl_text, create_tables) # this line writes the sfsql/create_tabled.sql file
            export_sql_script(copy_into_text, copy_into_tables) # this line write the sfsql/copy_into_tables.sql file
            ```
    * Check the SQL scripts were saved in the **`sfsql/`** folder and formatted correctly

4. **Create tables in Snowflake**
    * Copy+paste the contents of the **`sfsql/create_tables.sql`** file into a Snowflake worksheet
    * Execute the worksheet
    * Modify and troubleshoot as needed

5. **Export MSSQL data to CSV format**
    > Note: 🚧 This script is still in development and only works for tables with less than 1 million records
    > 
    * Update the Utils in **`scripts/utils.py`**:
        * Comment out or remove any tables with more than 1 million records from the list of tables in 
        * Also make sure to set the path where you want the csv files to be saved as `OUTPUT_DIR`
    * Make sure you are on a computer or VM that has access to the MSSQL database
    * Open **`scripts/export_mssql_to_csv.py`** and execute to write the MSSQL tables in compressed CSV format

6. **Load CSVs to S3**
    * Contact Kapil

7. **Load data from S3 to Snowflake**
    * Copy+paste the contents of the **`sfsql/create_tables.sql`** file into a Snowflake worksheet
    * Execute the worksheet
    * Modify and troubleshoot as needed


## **Issues ❗️**

If you encounter any issues or have questions, please raise an issue in the repository or contact the developer, Julie Scherer, at **[juliescherer@curo.com](mailto:juliescherer@curo.com)**. 📧