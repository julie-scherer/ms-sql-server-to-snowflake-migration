import os
import json

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

csv_path = os.path.join(os.getcwd(), 'ddl_to_dict', '{DATABASE_NAME}.csv') ## CSV exported from MSSQL with the table definitions
out_path = os.path.join(os.getcwd(), 'ddl_to_dict', '{DATABASE_NAME}_Utils.py')

def transform():
    db_dict = {}
    with open(csv_path, "r") as file:
        for line in file.readlines():
            L = line.split(',"')
            
            tbl = L[0].replace("\ufeff", "")
            schema = L[1].replace('"en-ci"',"").replace("NOT NULL", "").replace("NULL", "").replace("'","").replace('"','').replace("\n", "").replace(',',"").strip()
            print(f"{tbl} = {schema.split()}")

            tbl_dict = db_dict.get(tbl, {})

            # 1! Custom SQL file
            tbl_dict["sql"] = None

            # 2! Key column
            tbl_dict["key_column"] = schema.split()[0]

            # 3! Keys
            keys = [word for word in schema.split() if '_KEY' in word]
            tbl_dict["keys"] = f'{set(keys)}'

            # 4! Filtered by column
            tbl_dict["table_filtered_by"] = None
            if 'DATE_ENTERED' in schema:
                tbl_dict["table_filtered_by"] = "DATE_ENTERED"
            elif 'ENTERED_AT' in schema:
                tbl_dict["table_filtered_by"] = "ENTERED_AT"
            elif 'DATE_SENT' in schema:
                tbl_dict["table_filtered_by"] = "DATE_SENT"
            elif 'DATE_COMPLETED' in schema:
                tbl_dict["table_filtered_by"] = "DATE_COMPLETED"
            elif 'DATE_STARTED' in schema:
                tbl_dict["table_filtered_by"] = "DATE_STARTED"
            elif 'DATE_ENDED' in schema:
                tbl_dict["table_filtered_by"] = "DATE_ENDED"
            elif 'DATE_UPDATED' in schema:
                tbl_dict["table_filtered_by"] = "DATE_UPDATED"
            elif 'DATE_START' in schema:
                tbl_dict["table_filtered_by"] = "DATE_START"
            elif 'DATE_END' in schema:
                tbl_dict["table_filtered_by"] = "DATE_END"
            else:
                tbl_dict["table_filtered_by"] = None

            db_dict[f"{DATABASE_NAME}_{tbl.upper()}_HIST"] = tbl_dict

    if len(TABLES) != len(db_dict):
        print("[ERROR] - Dictionary missing tables")
    else:
        print("[SUCCESS] - All tables added")
    
    # print(f"fullTableList = {db_dict}")
    return db_dict


def export_dict():
    db_dict = transform()
    db_dict = json.dumps(db_dict, indent=2)
    result = db_dict.replace('false','False').replace('true','True').replace('null', 'None')
    return result

with open(f'{out_path}','+w') as file:
    fullTableList = export_dict()
    file.write(f"fullTableList = {fullTableList}")
    print("[SUCCESS] - File created")
