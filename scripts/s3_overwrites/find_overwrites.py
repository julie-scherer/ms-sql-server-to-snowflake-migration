import pandas as pd
from datetime import datetime

df = pd.read_csv('scripts/s3_overwrites/StagedData_20230818.csv')

print(df)
print(df.columns)
print(df['last_modified'])

for row in df.iterrows():
    print(row)



def convert_to_custom_format(date_string):
    # Parse the original date string into a datetime object
    dt_object = datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S %Z")
    # Convert the datetime object to the desired output format
    custom_format = dt_object.strftime("%Y-%m-%d")
    return custom_format

# Original date string
original_date_string = "Fri, 11 Aug 2023 19:10:19 GMT"
# Convert to custom format
custom_format_date = convert_to_custom_format(original_date_string)
print(custom_format_date)  # Output: 2023-08-11


# print(df[convert_to_custom_format(df['last_modified']) == '2023-08-11']['name'])



# Convert and filter dates
threshold_date = datetime.strptime("2023-08-14", "%Y-%m-%d")
filtered_names = []

for index, row in df.iterrows():
    custom_date = convert_to_custom_format(row['last_modified'])
    if custom_date > threshold_date.strftime("%Y-%m-%d"):
        filtered_names.append(row['name'])

print(filtered_names)

for file in filtered_names:
    print(file)


''' #! FILES THAT NEED TO BE RELOADED
s3://s3-dev-etldata-001/inbound/CREO/Backfill/Message/Message_Backfill_1.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/Message/Message_Backfill_2.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/Message/Message_Backfill_3.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_1.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2.csv
3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_3.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_4.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_5.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_6.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_7.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_8.csv
s3://s3-dev-etldata-001/inbound/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_9.csv
'''


for file in filtered_names:
    print(f"RM { file.replace('s3://s3-dev-etldata-001/inbound','@ETL.INBOUND') };")


''' #! FILES THAT NEED TO BE REMOVED
RM @ETL.INBOUND/CREO/Backfill/Message/10_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/11_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/12_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/13_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/14_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/15_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/16_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/17_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/18_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/19_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/1_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/20_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/21_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/22_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/23_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/24_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/25_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/26_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/27_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/28_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/29_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/2_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/30_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/31_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/32_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/3_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/4_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/5_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/6_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/7_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/8_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/9_Message_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/Message_Backfill_1.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/Message_Backfill_2.csv;
RM @ETL.INBOUND/CREO/Backfill/Message/Message_Backfill_3.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/10_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/11_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/12_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/13_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/14_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/15_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/16_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/17_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/18_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/19_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/1_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/2_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/3_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/4_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/5_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/6_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/7_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/8_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/9_MessageDeliveryStatus_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_1.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_1.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_2.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_3.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_4.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_5.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_6.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_7.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_2023-08-18_8.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_3.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_4.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_5.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_6.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_7.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_8.csv;
RM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/MessageDeliveryStatus_Backfill_9.csv;
RM @ETL.INBOUND/CREO/Backfill/MessagePartV2/1_MessagePartV2_Backfill.csv;
RM @ETL.INBOUND/CREO/Backfill/MessagePartV2/2_MessagePartV2_Backfill.csv;
'''