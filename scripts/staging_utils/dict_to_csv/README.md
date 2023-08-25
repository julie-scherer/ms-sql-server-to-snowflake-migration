# Create `ETL.Utils` tables in Snowflake

1. Run the **`staging_utils/get_dict/create_creo_utils_dict.py`** script to export the MSSQL table definitions in the csv file to a Python dictionary
2. Copy the dictionary output in **`CREO_Utils.py`** into **`staging_utils/utils/Staging_CREO_utils.py`**
3. Run the **`staging_utils/export_json_to_csv.py`** script to create a CSV to load into Snowflake

```bash
cd /Users/juliescherer/code/temp/staging_utils
python3 -m venv .venv
source .venv/bin/activate
pip install pandas
python export_json_to_csv.py
```

4. Go to [Staging UTILS folder](https://app.snowflake.com/curoorg/curo/#/staging_utils-fG0Adr5el) in Snowflake and run [UTILS Create Tables](https://app.snowflake.com/curoorg/curo/w46Rad6sOJ5A#query) worksheet
5. Go to ARES > ETL > UTILS_CREO (for example), click Load Data, load CSV from **`csv/`** folder, click Next
    * Under File format, select CSV:
        * Skip header = True
        * Field deliminater = |
6. Open [UTILS Copy Into](https://app.snowflake.com/curoorg/curo/w5BZCI8xcpqc#query) worksheet and run **`COPY INTO`** query
7. Go back to data preview and check the csv was loaded correctly (e.g., see [here](https://app.snowflake.com/curoorg/curo/#/data/databases/ARES/schemas/ETL/table/UTILS_CREO/data-preview) for CREO)