
class Utils:
    ## Microsoft SQL server
    MSSQL_SERVER='rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'

    ## Path to export results
    # OUTPUT_DIR = '\\ictfs01\SharedUSA\IT\Batch\DW\BCP' # shared drive
    # OUTPUT_DIR = fr"C:\Users\JulieScherer\Creo-JS\CSVFiles" # pc local
    OUTPUT_DIR=fr"C:\Users\JulieScherer\OneDrive - curo.com\Creo-JS\CSVFiles" # pc onedrive

    MAX_WORKERS=7
    TIMEOUT_SECONDS=300

    DATABASE1=('CREO', ['Log', 'Message', 'Log', 'Message', 'Package',  'Rule','User', 'Config', 'Template', 'Container', 'PackageTemplate', 'TemplateRule', 'Campaign', 'Communication', 'CommunicationMailing', 'Dataset', 'DatasetCell', 'DatasetColumn', 'DatasetRow', 'DatasetValue', 'Datasource', 'DeliveryStatus', 'Emoji', 'Folder', 'MessageDeliveryStatus', 'FolderContact', 'FolderMessage', 'Parameter', 'MessageStatusQueue', 'Global', 'CampaignType', 'TempMessage', 'DeadMessages', 'ContactType', 'ApprovalRequest', 'MessageContactType', 'ApprovalRequestItem', 'MessageType', 'ConfigHistory', 'TemplateType', 'Contact', 'MessageContact', 'MessagePart', 'MessageContactV2', 'MessagePartV2', 'DeadMessages2', 'WebHook'] )
    DATABASE2=('CREOArchive', ['Message', 'Message', 'DatasetRow', 'MessageContact', 'DatasetCell', 'Global', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2'])
    DATABASE3=('CREOArchive2', ['Message', 'Message', 'DatasetRow', 'MessageContact', 'DatasetCell', 'Global', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePart', 'MessagePartV2'])

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
