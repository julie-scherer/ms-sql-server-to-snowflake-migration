import pandas as pd
from pathlib import Path
import json


file = 'staging_utils/csv_to_dict/CREO_UTILS.csv'
df = pd.read_csv(file)
# print(df)

# json_file = 'staging_utils/csv_to_dict/fullTableList.json'
# Path(json_file).touch(exist_ok=True)
# json_df = df.to_json(json_file)
# print(f"JSON dataframe: {json_df}")

# out = 'staging_utils/csv_to_dict/fullTableList.py'
# Path(out).touch(exist_ok=True)
# with open(out, "w") as f:
#     dict_df = df.to_dict()
#     f.write(f"{dict_df}")

dict_df = df.to_dict()
# print(json.dumps(dict_df, indent=2))
# print(f"Dict dataframe: {dict_df}")

# fullTableList = {
#     'TABLE_NAME': '',
#     'SQL': '',
#     'KEY_COLUMN': '',
#     'KEYS': '',
#     'TABLE_FILTERED_BY': '',
# }

# count = 0
temp = {}
for count in range(45):
    for key, value in dict_df.items():

        # print(f"\n\nKey: {key}")
        # print(f"Value: {value}")
        
        if key == 'TABLE_NAME':
            table_name = value[count]
            # print(f"TABLE_NAME: {table_name}")
            temp[table_name] = {}
        
        elif key == 'SQL':
            sql = value[count]
            # print(f"SQL: {sql}")
            temp[table_name]['SQL'] = sql
        
        elif key == 'KEY_COLUMN':
            key_column = value[count]
            # print(f"KEY_COLUMN: {key_column}")
            temp[table_name]['KEY_COLUMN'] = key_column
        
        elif key == 'KEYS':
            keys = value[count]
            # print(f"KEYS: {keys}")
            if isinstance(keys,str):
                keys = keys.replace("'","").replace('"','').strip("{}").split(", ")
            temp[table_name]['KEYS'] = keys
        
        elif key == 'TABLE_FILTERED_BY':
            table_filtered_by = value[count]
            # print(f"TABLE_FILTERED_BY: {table_filtered_by}")
            temp[table_name]['TABLE_FILTERED_BY'] = table_filtered_by


print(json.dumps(temp, indent=2))
    