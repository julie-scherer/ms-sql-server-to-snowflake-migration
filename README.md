# SQL Server to Snowflake Data Definition and Loading Migration ✨

This repository contains scripts and instructions to automate many of the steps involved in the migration of data from Microsoft SQL Server to Snowflake data warehouse. The process involves exporting data from SQL Server, converting table definitions to Snowflake syntax, and loading data into Snowflake. 🚀

## **Setup ⚙️**

1. **Create a virtual environment** by executing the following commands:
    
    ```bash
    python -m venv .venv  # for Windows
    # python3 -m venv .venv  # for macOS
    source .venv/bin/activate
    pip install --upgrade pip -r requirements.txt
    ```
    

## **Migration Steps 📝**

1. **IN DEVELOPMENT**: Run the **`scripts/1_*.py`** Python script on the MSSQL server to export data from MSSQL database(s) to CSV file(s). Make sure to change the database at the top of the script accordingly.
    
    > Note: 🚧 This script is still under development. For now, you can use sqlcmd or invoke-sqlcmd to export data from MSSQL to CSV.
    > 
2. Run the **`mssql/2_*.sql`** query in MSSQL to retrieve table names and table definitions (with MSSQL data types changed to Snowflake data types). Export the query results for each database as an Excel file. Save the Excel file(s) in the **`data/`** folder with the same name as the database.
3. Run the **`scripts/3_*.py`** Python script to generate **`CREATE TABLE`** and **`COPY INTO`** Snowflake scripts. You may need to modify the code slightly to achieve the desired format and results. The formatted SQL scripts will be exported to the **`sfsql/`** folder.
4. Copy and paste the **`CREATE TABLE`** and **`COPY INTO`** SQL queries into a Snowflake worksheet. Execute the scripts in the Snowflake UI for further troubleshooting and execution.

## **Issues ❗️**

If you encounter any issues or have questions, please raise an issue in the repository or contact the developer, Julie Scherer, at **[juliescherer@curo.com](mailto:juliescherer@curo.com)**. 📧