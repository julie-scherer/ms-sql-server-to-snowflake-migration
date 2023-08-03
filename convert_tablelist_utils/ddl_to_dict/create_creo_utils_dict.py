import json
import re

example = {
    "table_name": "",
    "sql": None,
    "table_filtered_by": True,
    "key_column": None
}

def transform():
    file = 'staging_utils/ddl_to_dict/CREO.csv'
    creo_dict = {}
    tables = ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication', 'CommunicationMailing', 'Config', 'ConfigHistory', 'Contact', 'ContactType', 'Container', 'Dataset', 'DatasetCell', 'DatasetColumn', 'DatasetRow', 'DatasetValue', 'Datasource', 'DeadMessages', 'DeadMessages2', 'DeliveryStatus', 'Emoji', 'Folder', 'FolderContact', 'FolderMessage', 'Global', 'Log', 'Message', 'MessageContact', 'MessageContactType', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2', 'MessageStatusQueue', 'MessageType', 'Package', 'PackageTemplate', 'Parameter', 'Rule', 'Template', 'TemplateRule', 'TemplateType', 'TempMessage', 'User', 'WebHook']
    with open(file, "r") as file:
        # print(file)
        for line in file.readlines():
            # print(line)

            L = line.split(',"')
            # print(L)
            
            tbl = L[0].replace("\ufeff", "")
            schema = L[1].replace("'","").replace('"','').replace("\n", "")
            # print(f"{tbl} = {schema}")

            tbl_dict = creo_dict.get(tbl, {})

            tbl_dict["sql"] = None

            if 'DATE_ENTERED' in schema:
                tbl_dict["table_filtered_by"] = "DATE_ENTERED"
            elif 'ENTERED_AT' in schema:
                tbl_dict["table_filtered_by"] = "ENTERED_AT"
            elif 'DATE_SENT' in schema:
                tbl_dict["table_filtered_by"] = "DATE_SENT"
            elif 'DATE_START' in schema:
                tbl_dict["table_filtered_by"] = "DATE_START"
            elif 'DATE_END' in schema:
                tbl_dict["table_filtered_by"] = "DATE_END"
            else:
                tbl_dict["table_filtered_by"] = None
            
            word_parts = re.findall('[A-Z][^A-Z]*', tbl)
            hyphenated_tbl = '_'.join(word_parts).upper() # Join the parts with underscores and convert to uppercase
            hyphenated_key = hyphenated_tbl + "_KEY" # Add "_KEY" at the end
            if hyphenated_key in schema:
                tbl_dict["key_column"] = hyphenated_key
            else:
                tbl_dict["key_column"] = None
            
            creo_dict[f"CREO_{tbl.upper()}_HIST"] = tbl_dict

    if len(tables) != len(creo_dict):
        print("[ERROR] - Dictionary missing tables")
    else:
        print("[SUCCESS] - All tables added")
    
    # print(f"fullTableList = {creo_dict}")
    return creo_dict


def export_dict():
    creo_dict = transform()
    res = json.dumps(creo_dict, indent=2)
    fres = res.replace('false','False').replace('true','True').replace('null', 'None')
    return fres

with open('staging_utils/ddl_to_dict/CREO_Utils.py','+w') as file:
    fullTableList = export_dict()
    file.write(f"fullTableList = {fullTableList}")
    print("[SUCCESS] - File created")
