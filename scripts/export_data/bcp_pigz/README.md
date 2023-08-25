# SQL Server to Snowflake Data Definition and Loading Migration ✨

This repository contains scripts and instructions to automate many of the steps involved in the migration of data from Microsoft SQL Server to Snowflake data warehouse. The process involves exporting data from SQL Server, converting table definitions to Snowflake syntax, and loading data into Snowflake. 🚀

## **Getting started**

### Prerequisites

To run this repo, you need:
- Python installed
- Access to a MSSQL server and database
- Access to Snowflake

## **Setting up your local environment**

### Step 1: Create a virtual environment to run Python scripts
Follow the steps below from the **`CURO-Astro/`** directory on your local computer:
1. navigate to project folder
    ```bash
    cd mssql-snowflake-migration 
    ```
2. create a virtual environment (skip if you've already walked through these steps and created a .venv folder)
    ```bash
    python -m venv .venv  # for Windows
    python3 -m venv .venv  # for macOS
    ```
3. activate the virtual environment
    ```bash
    source .venv/bin/activate
    ```
4. install the necessary python packages (skip if you've already done this when going through the steps before)
    ```bash
    pip install --upgrade pip -r requirements.txt --no-cache
    pip list # check packages installed correctly
    ```

### Step 2: Update global variables and configurations in the repo
1. Navigate to project folder from the `Curo-Astro` root directory
    ```bash
    cd mssql-snowflake-migration 
    ```
2. Open **`scripts/utils.py`** and update **`MSSQL_SERVER`**, **`PROJECT_DIR`**, **`OUTPUT_DIR`**, and **`BATCH`** under the Utils class 
    > Please do not change the names of these variables, only the values, as the variables are used across multiple scripts
    >
    * More about **`BATCH`**: 
        * This is a list of variables which are also defined in Utils. It includes all the MSSQL databases and corresponding tables you are running in the current batch.
        * In some cases, you may need to export data from multiple different MSSQL databases, as well as copy the different data source into the same Snowflake table. While this won't make a difference in terms of how you create the Snowflake tables, it will affect how you export the data from MSSQL and write the **`COPY INTO`** scripts. Therefore, to maintain consistency across all the scripts, you will need to follow the same format. 
        Take the following example:
            ```python
            B1_CREO = ('CREO', ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication'])
            B1_CREOArchive = ('CREOArchive', ['Global']) # 1 record
            BATCH = [ B1_CREO, B1_CREOArchive ]
            ```
            First, you are expected to define a tuple, with the name of the MSSQL database as the first element and a list of the corresponding table names as the second element. Then, you would add that variable to the **`BATCH`** list.
        * A more common use case would look like this:
            ```python
            B1_CREO = ('CREO', ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication'])
            B2_CREO = ('CREO', ['DatasetCell', 'DatasetValue', 'MessageDeliveryStatus'])
            BATCH = [ B1_CREO ]
            ```
            While it seems silly or inconvenient to define **`BATCH`** this way, please do so anyway to ensure the program runs as intended. You can predefine all your batches like in this example, but whenever you actually run batch, be sure to update the value inside the **`BATCH`** list.

## **Migration Steps 📝**

### Step 1: Export table definitions from MSSQL
* Open the **`mssql-snowflake-migration/mssql/get_tbl_definitions.sql`** file in SSMS, Azure Data Studio, or whatever you use to connect to the MSSQL database
* Connect to the MSSQL server
* Update the **`USE database;`** line at the top of the script for your use case
* Execute the SQL file and check the query result
    * You should see the table names and table definitions with MSSQL data types changed to Snowflake
* Export the query result as a CSV file and save the CSV file in the **`data/`** folder with the same name as your database

### Step 2: Create tables in Snowflake

#### Step 2.1: Generate `CREATE TABLE` script
* Navigate to the project folder in the command line (**`cd mssql-snowflake-migration`**)
* Open the Python file in **`scripts/write_sf_ddl_worksheet.py`** and update the values at the beginning of the script
* Run the command below to execute the Python script and generate the **`CREATE TABLE`** sql file:
    ```bash
    ## CREATE TABLES
    python scripts/write_sf_ddl_worksheet.py # PC/Windows
    python3 scripts/write_sf_ddl_worksheet.py # Mac
    ```
* Check that the SQL file was saved in the **`sfsql/`** folder and formatted correctly

#### Step 2.2: Create tables in Snowflake
* Copy+paste the contents of the **`sfsql/create_tables.sql`** file into a Snowflake worksheet
* Execute the worksheet
* Modify and troubleshoot as needed


### Step 3: Export data from MSSQL database 
#### Step 3.1: Export data in CSV format
> 🚧 This script is still in development and only works for tables with less than 1 million records
> 
* Navigate to the project folder in the command line (**`cd mssql-snowflake-migration`**)
* Update the Utils in **`scripts/utils.py`**:
    * Comment out or remove any tables with more than 1 million records from the list of tables in 
    * Check that **`OUTPUT_DIR`** is the path where you want to save the csv files
* Make sure you are on a computer or VM that has access to the MSSQL database
* Run the command below to execute the Python script to write the MSSQL tables in compressed CSV format:
    ```bash
    python scripts/export_mssql_to_csv.py # PC/Windows
    python3 scripts/export_mssql_to_csv.py # Mac
    ```
#### Step 3.2: Load CSVs to S3
* Contact Kapil


### Step 4: Copy CSVs into Snowflake
#### Step 4.1: Generate `COPY INTO` script
* Navigate to the project folder in the command line (**`cd mssql-snowflake-migration`**)
* Open the Python file in **`scripts/write_sf_copy_into_worksheet.py`** and update the values at the beginning of the script
* Run the command below to execute the Python script and generate the **`COPY INTO`** sql file:
    ```bash
    ## COPY INTO
    python scripts/write_sf_copy_into_worksheet.py # PC/Windows
    python3 scripts/write_sf_copy_into_worksheet.py # Mac
    ```
* Check that the SQL file was saved in the **`sfsql/`** folder and formatted correctly

### Step 4.2: Copy data from S3 to Snowflake
* Copy+paste the contents of the **`sfsql/create_tables.sql`** file into a Snowflake worksheet
* Execute the worksheet
* Modify and troubleshoot as needed



## **Issues ❗️**

If you encounter any issues or have questions, please raise an issue in the repository or contact the developer, Julie Scherer, at **[juliescherer@curo.com](mailto:juliescherer@curo.com)**. 📧