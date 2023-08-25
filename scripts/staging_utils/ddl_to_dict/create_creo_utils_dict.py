import os
import json
import pandas as pd

'''
example = {
        "CREO_APPROVALREQUEST_HIST": {
        "sql": None,
        "key_column": "APPROVAL_REQUEST_KEY",
        "keys": "{'APPROVAL_REQUEST_KEY', 'PACKAGE_KEY'}",
        "table_filtered_by": "ENTERED_AT",
    }
}
'''

DATABASE_NAME = 'CREO'
TABLES = ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication', 'CommunicationMailing', 'Config', 'ConfigHistory', 'Contact', 'ContactType', 'Container', 'Dataset', 'DatasetCell', 'DatasetColumn', 'DatasetRow', 'DatasetValue', 'Datasource', 'DeadMessages', 'DeadMessages2', 'DeliveryStatus', 'Emoji', 'Folder', 'FolderContact', 'FolderMessage', 'Global', 'Log', 'Message', 'MessageContact', 'MessageContactType', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2', 'MessageStatusQueue', 'MessageType', 'Package', 'PackageTemplate', 'Parameter', 'Rule', 'Template', 'TemplateRule', 'TemplateType', 'TempMessage', 'User', 'WebHook']

out_path = os.path.join(os.getcwd(), 'ddl_to_dict', f'{DATABASE_NAME}_Utils.py')


# csv_path = os.path.join(os.getcwd(), 'ddl_to_dict', f'{DATABASE_NAME}.csv') ## CSV exported from MSSQL with the table definitions
# def transform_strarr():
#     db_dict = {}
#     with open(csv_path, "r") as file:
#         for line in file.readlines():
#             L = line.split(',"')
            
#             tbl = L[0].replace("\ufeff", "")
#             columns = L[1].replace('"en-ci"',"").replace("NOT NULL", "").replace("NULL", "").replace("'","").replace('"','').replace("\n", "").replace(',',"").strip()
#             # print(f"{tbl} = {columns.split()}")

#             tbl_dict = db_dict.get(tbl, {})

#             # 1! Custom SQL file
#             tbl_dict["sql"] = None

#             # 2! Key column
#             tbl_dict["key_column"] = columns.split()[0]

#             # 3! Keys
#             # keys = set([word for word in columns.split() if '_KEY' in word])
#             keys_list = [f'{word}' for word in columns.split() if '_KEY' in word]
#             keys = f"{set(keys_list)}" if keys_list else None
#             tbl_dict["keys"] = keys

#             # 4! Filtered by column
#             tbl_dict["table_filtered_by"] = None
#             if 'DATE_ENTERED' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_ENTERED"
#             elif 'ENTERED_AT' in columns:
#                 tbl_dict["table_filtered_by"] = "ENTERED_AT"
#             elif 'DATE_SENT' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_SENT"
#             elif 'DATE_COMPLETED' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_COMPLETED"
#             elif 'DATE_STARTED' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_STARTED"
#             elif 'DATE_ENDED' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_ENDED"
#             elif 'DATE_UPDATED' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_UPDATED"
#             elif 'DATE_START' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_START"
#             elif 'DATE_END' in columns:
#                 tbl_dict["table_filtered_by"] = "DATE_END"
#             else:
#                 tbl_dict["table_filtered_by"] = None

#             db_dict[f"{DATABASE_NAME}_{tbl.upper()}_HIST"] = tbl_dict

#     if len(TABLES) != len(db_dict):
#         print("[ERROR] - Dictionary missing tables")
#     else:
#         print("[SUCCESS] - All tables added")
    
#     # print(f"fullTableList = {db_dict}")
#     return db_dict

import json
csv_path = os.path.join(os.getcwd(), 'ddl_to_dict', f'{DATABASE_NAME}_DDL.csv') ## CSV exported from MSSQL with the table definitions
def transform():
    # Create an empty dictionary for the database, which will store all the tables' dictionaries
    database_dict = {}
    ddl_table = pd.read_csv(csv_path)
    print(ddl_table)

    table_names = set(ddl_table['TABLE_NAME'])
    print(f"table names: \n{table_names}\n")

    # Loop through each table in table_names
    for tbl in table_names:
        # Create an empty dictionary to store the `sql`, `key_column`, `keys`, and `table_filtered_by` key-value pairs for each table
        table_dict = database_dict.get(tbl, {})

        ## 1: Custom SQL file
        table_dict["sql"] = None # assume there are no custom sql files and set to None for now (might be changed later)
        
        ## 2: Primary column
        key_column = '' # get key column using mssql query
        table_dict["key_column"] = key_column

        columns = [val.strip('"') for val in ddl_table[ddl_table['TABLE_NAME'] == tbl]['COLUMN_NAME'].values]
        print(f"columns = {columns}")
        
        keys_list = [col for col in columns if '_KEY' in col]
        keys = f"{set(keys_list)}" if keys_list else None
        table_dict["keys"] = keys

        ## 4: Filtered by column
        if 'DATE_ENTERED' in columns:
            table_dict["table_filtered_by"] = "DATE_ENTERED"
        elif 'ENTERED_AT' in columns:
            table_dict["table_filtered_by"] = "ENTERED_AT"
        elif 'DATE_SENT' in columns:
            table_dict["table_filtered_by"] = "DATE_SENT"
        elif 'DATE_COMPLETED' in columns:
            table_dict["table_filtered_by"] = "DATE_COMPLETED"
        elif 'DATE_STARTED' in columns:
            table_dict["table_filtered_by"] = "DATE_STARTED"
        elif 'DATE_ENDED' in columns:
            table_dict["table_filtered_by"] = "DATE_ENDED"
        elif 'DATE_UPDATED' in columns:
            table_dict["table_filtered_by"] = "DATE_UPDATED"
        elif 'DATE_START' in columns:
            table_dict["table_filtered_by"] = "DATE_START"
        elif 'DATE_END' in columns:
            table_dict["table_filtered_by"] = "DATE_END"
        else:
            table_dict["table_filtered_by"] = None
        
        # SNOWFLAKE_TABLE = f"CREO_{tbl.upper()}_HIST"
        database_dict[tbl] = table_dict
    
    if len(table_names) != len(database_dict):
        print("[ERROR] - Dictionary missing tables")
    else:
        print("[SUCCESS] - All tables added")
    
    # print(f"Database dictionary: {database_dict}")
    return database_dict

def export_dict():
    db_dict = transform()
    db_dict = json.dumps(db_dict, indent=2)
    result = db_dict.replace('false','False').replace('true','True').replace('null', 'None')
    with open(f'{out_path}','+w') as file:
        file.write(f"fullTableList = {result}")
        print("[SUCCESS] - File created")
    return result


export_dict()

# print(transform())