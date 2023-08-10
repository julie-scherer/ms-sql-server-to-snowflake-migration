'''
Features
SQL driven : move full table lists from Staging DAGs to ARES.ETL.Utils tables for CREO, CM, and LD
Exporting data using BCP
Compressing data using PigZ
'''

## Connect to ETL.Utils
SF_CONN = "snowflake_default"
sf_hook = SnowflakeHook(snowflake_conn_id=SF_CONN)

'''
** Instead of getting data from dictionary, get these fields from ETL.UTILS table **
[LINE 111]
    table_list_name = fullTableList.get(table_name)
    if table_list_name: #// Use "if" statement to avoid Key error
        sql_file = table_list_name.get("sql")
        has_date_entered = table_list_name.get("has_date_entered")
        key_column = table_list_name.get("key_column", False) 
'''

# Connect to snowflake and get the data 
sf_hook.get_first(f"""
                  SELECT * 
                  FROM ARES.ETL.UTILS 
                  WHERE TABLE_NAME ILIKE '%{table_name.upper()}%'
                  """)