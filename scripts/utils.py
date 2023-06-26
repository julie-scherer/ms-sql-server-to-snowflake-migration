
class Utils:
    ## Microsoft SQL server
    MSSQL_SERVER = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'

    ## Path to root directory of project 
    PROJECT_DIR = fr"C:\Users\JulieScherer\OneDrive - curo.com\migration-automation-pipeline"

    ## Path to export results
    # OUTPUT_DIR = '\\ictfs01\SharedUSA\IT\Batch\DW\BCP' # team shared drive
    OUTPUT_DIR = fr"C:\Users\JulieScherer\OneDrive - curo.com\migration-automation-pipeline\csvs" # onedrive on PC/VM

    ## Databases and table names -- ('database_name', ['column_name', 'column_name', ... ])
    # ** ONLY USE TABLES WITH LESS THAN MILLION RECORDS **
    DATABASE1 = ('CREO', ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication', 'CommunicationMailing', 'Config', 'ConfigHistory', 'ContactType', 'Container', 'Dataset', 'DatasetColumn', 'Datasource', 'DeadMessages', 'DeadMessages2', 'DeliveryStatus', 'Emoji', 'Folder', 'FolderContact', 'FolderMessage', 'Global', 'Log', 'MessageContact', 'MessageContactType', 'MessagePart',  'MessageStatusQueue', 'MessageType', 'Package', 'Parameter', 'Rule', 'Template', 'TemplateRule', 'TemplateType', 'TempMessage', 'User', 'WebHook'])
    # // DATABASE1_OG = ('CREO', ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication', 'CommunicationMailing', 'Config', 'ConfigHistory', 'Contact', 'ContactType', 'Container', 'Dataset', 'DatasetCell', 'DatasetColumn', 'DatasetRow', 'DatasetValue', 'Datasource', 'DeadMessages', 'DeadMessages2', 'DeliveryStatus', 'Emoji', 'Folder', 'FolderContact', 'FolderMessage', 'Global', 'Log', 'Message', 'MessageContact', 'MessageContactType', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2', 'MessageStatusQueue', 'MessageType', 'Package', 'PackageTemplate', 'Parameter', 'Rule', 'Template', 'TemplateRule', 'TemplateType', 'TempMessage', 'User', 'WebHook'])
    # // DATABASE1_TBLS_REMOVED = ['Contact', 'DatasetCell', 'DatasetRow', 'DatasetValue', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2', 'PackageTemplate' ]
    DATABASE2 = ('CREOArchive', ['Global', 'MessageContact', 'MessagePart'] )
    # // DATABASE2_OG = ('CREOArchive', ['DatasetCell', 'DatasetRow', 'Global', 'Message', 'MessageContact', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2'] )
    DATABASE3 = ('CREOArchive2', ['Global', 'MessageContact', 'MessagePart'] )
    # // DATABASE3_OG = ('CREOArchive2', ['DatasetCell', 'DatasetRow', 'Global', 'Message', 'MessageContact', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2'] )

    MAX_WORKERS = 7
    TIMEOUT_SECONDS = 90



def create_tbl_syntax(table_name, schema):
    return f"""

-- // TABLE: {table_name}
-- !! DROP TABLE IF EXISTS STG.CREO_{table_name.upper()}_HIST;
CREATE TABLE IF NOT EXISTS STG.CREO_{table_name.upper()}_HIST ( 
    FILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    {schema}
);
-- // [STATUS=tbd] : SELECT * FROM STG.CREO_{table_name}_HIST LIMIT 10;

"""

def copy_into_tbl_syntax(table_name, casted_cols):
    return f"""

-- // SELECT DEV_JS.ETL.COPYSELECT('STG','CREO_{table_name}_HIST',3);
COPY INTO DEV_JS.STG.CREO_{table_name}_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-01'), 
        {casted_cols}
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = ARES.STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/{table_name}/{table_name}_Backfill.csv.gz*';
-- // [STATUS=tbd] : SELECT * FROM STG.CREO_{table_name}_HIST LIMIT 10;

"""
