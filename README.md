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

1. **IN DEVELOPMENT**: Run the **`scripts/1_*.py`** Python script on the MSSQL server to export data from MSSQL database(s) to CSV file(s).
    
    > Note: 🚧 This script is still under development. For now, you can use sqlcmd or invoke-sqlcmd to export data from MSSQL to CSV.
    > 
2. Connect to the MSSQL database and run the **`mssql/2_*.sql`** query to retrieve table names and table definitions (with MSSQL data types changed to Snowflake data types). Make sure to change **`USE database;`** at the top of the script accordingly. Run the script (you may need to re-run **`SELECT * from #TempTable`** once it's done to get a clean view of the query results). Export the query result as an Excel file and save the Excel file in the **`data/`** folder with the same name as your database.
3. Run the **`scripts/3_*.py`** Python script to generate **`CREATE TABLE`** and **`COPY INTO`** Snowflake scripts. You may need to modify the code slightly to achieve the desired format and results. The formatted SQL scripts will be exported to the **`sfsql/`** folder.
4. Copy and paste the **`CREATE TABLE`** and **`COPY INTO`** SQL queries into a Snowflake worksheet. Execute the scripts in the Snowflake UI for further troubleshooting and execution.

## **Issues ❗️**

If you encounter any issues or have questions, please raise an issue in the repository or contact the developer, Julie Scherer, at **[juliescherer@curo.com](mailto:juliescherer@curo.com)**. 📧