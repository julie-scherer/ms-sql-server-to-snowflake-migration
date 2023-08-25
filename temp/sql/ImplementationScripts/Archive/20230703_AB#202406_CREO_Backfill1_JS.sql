
-- // TABLE 1: ApprovalRequest
COPY INTO STG.CREO_ApprovalRequest_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: APPROVAL_REQUEST_KEY INT NOT NULL
		($2)::int, 	-- $2: STATUS INT NOT NULL
		to_timestamp_ntz($3), 	-- $3: ENTERED_AT TIMESTAMP_LTZ NOT NULL
		($4)::int, 	-- $4: PACKAGE_KEY INT NOT NULL
		($5)::varchar, 	-- $5: REQUESTED_BY VARCHAR(8000) NOT NULL
		($6)::varchar, 	-- $6: PROCESSED_BY VARCHAR(8000) NULL
		($7)::varchar, 	-- $7: COMMIT_MESSAGE VARCHAR(8000) NULL
		to_timestamp_ntz($8) 	-- $8: PROCESSED_AT TIMESTAMP_LTZ NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/ApprovalRequest/ApprovalRequest_Backfill.csv.gz*';

-- // TABLE 2: ApprovalRequestItem
COPY INTO STG.CREO_ApprovalRequestItem_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: APPROVAL_REQUEST_ITEM_KEY INT NOT NULL
		($2)::int, 	-- $2: APPROVAL_REQUEST_KEY INT NOT NULL
		($3)::int 	-- $3: TEMPLATE_KEY INT NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/ApprovalRequestItem/ApprovalRequestItem_Backfill.csv.gz*';

-- // TABLE 3: Campaign
COPY INTO STG.CREO_Campaign_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: CAMPAIGN_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NULL
		($3)::int, 	-- $3: CONTAINER_KEY INT NOT NULL
		($4)::int, 	-- $4: DATASET_KEY INT NULL
		($5)::boolean, 	-- $5: IS_DELETED BOOLEAN NOT NULL
		($6)::boolean, 	-- $6: IS_ENABLED BOOLEAN NOT NULL
		($7)::boolean, 	-- $7: IS_PRODUCTION BOOLEAN NOT NULL
		to_timestamp_ntz($8), 	-- $8: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($9)::varchar, 	-- $9: AUTO_START_PATTERN_FOR_DATASET VARCHAR(8000) NULL
		($10)::boolean, 	-- $10: DISALLOW_RENAME BOOLEAN NOT NULL
		($11)::boolean, 	-- $11: IS_SEED_LIST_ENABLED BOOLEAN NOT NULL
		($12)::int, 	-- $12: SEED_LIST_DATASET_KEY INT NULL
		($13)::int, 	-- $13: SEED_LIST_FREQUENCY_DAYS INT NULL
		($14)::time, 	-- $14: SEED_LIST_START_TIME TIME NULL
		to_timestamp_ntz($15), 	-- $15: SEED_LIST_SENT_AT TIMESTAMP_LTZ NULL
		to_timestamp_ntz($16), 	-- $16: SEED_LIST_WILL_SEND_AT TIMESTAMP_LTZ NULL
		($17)::int, 	-- $17: CAMPAIGN_TYPE_KEY INT NULL
		($18)::boolean, 	-- $18: IS_PRIORITY BOOLEAN NOT NULL
		($19)::boolean, 	-- $19: SEND_WITHOUT_PREFERENCES BOOLEAN NOT NULL
		($20)::varchar 	-- $20: REASON_DISABLED VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Campaign/Campaign_Backfill.csv.gz*';

-- // TABLE 4: CampaignType
COPY INTO STG.CREO_CampaignType_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: CAMPAIGN_TYPE_KEY INT NOT NULL
		($2)::varchar 	-- $2: NAME VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/CampaignType/CampaignType_Backfill.csv.gz*';

-- // TABLE 5: Communication
COPY INTO STG.CREO_Communication_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: COMMUNICATION_KEY INT NOT NULL
		($2)::int, 	-- $2: CAMPAIGN_KEY INT NOT NULL
		($3)::boolean, 	-- $3: IS_ENABLED BOOLEAN NOT NULL
		($4)::boolean, 	-- $4: IS_DELETED BOOLEAN NOT NULL
		to_timestamp_ntz($5), 	-- $5: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($6)::time, 	-- $6: START_TIME TIME NULL
		($7)::time, 	-- $7: END_TIME TIME NULL
		($8)::int, 	-- $8: LIMIT_PER_HOUR INT NULL
		to_timestamp_ntz($9), 	-- $9: DATE_START TIMESTAMP_LTZ NULL
		to_timestamp_ntz($10), 	-- $10: DATE_END TIMESTAMP_LTZ NULL
		($11)::int, 	-- $11: WEEKDAYS INT NULL
		($12)::varchar, 	-- $12: TEMPLATE_PATH VARCHAR(8000) NULL
		($13)::varchar, 	-- $13: CONFIG_PATH VARCHAR(8000) NULL
		($14)::smallint, 	-- $14: TYPE SMALLINT NOT NULL
		($15)::boolean, 	-- $15: ENABLE_QUEUE_BEFORE_DATE_START BOOLEAN NULL
		($16)::boolean, 	-- $16: IS_PRODUCTION BOOLEAN NOT NULL
		($17)::varchar, 	-- $17: Provider VARCHAR(8000) NULL
		($18)::varchar 	-- $18: REASON_DISABLED VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Communication/Communication_Backfill.csv.gz*';

-- // TABLE 6: CommunicationMailing
COPY INTO STG.CREO_CommunicationMailing_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: COMMUNICATION_MAILING_KEY INT NOT NULL
		($2)::int, 	-- $2: COMMUNICATION_KEY INT NOT NULL
		($3)::int, 	-- $3: SENT INT NOT NULL
		($4)::int, 	-- $4: QUEUED INT NOT NULL
		($5)::int, 	-- $5: TOTAL INT NOT NULL
		($6)::int, 	-- $6: EXCEPTION INT NOT NULL
		($7)::int, 	-- $7: STATUS INT NOT NULL
		($8)::boolean, 	-- $8: IS_STALE BOOLEAN NOT NULL
		to_timestamp_ntz($9), 	-- $9: DATE_STARTED TIMESTAMP_LTZ NULL
		to_timestamp_ntz($10), 	-- $10: DATE_COMPLETED TIMESTAMP_LTZ NULL
		to_timestamp_ntz($11) 	-- $11: DATE_UPDATED TIMESTAMP_LTZ NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/CommunicationMailing/CommunicationMailing_Backfill.csv.gz*';

-- // TABLE 7: Config
COPY INTO STG.CREO_Config_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: CONFIG_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		($3)::varchar 	-- $3: VALUE VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Config/Config_Backfill.csv.gz*';

-- // TABLE 8: ConfigHistory
COPY INTO STG.CREO_ConfigHistory_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: CONFIG_HISTORY_KEY INT NOT NULL
		($2)::int, 	-- $2: CONFIG_KEY INT NOT NULL
		($3)::varchar, 	-- $3: NAME VARCHAR(8000) NOT NULL
		($4)::varchar, 	-- $4: OLD_VALUE VARCHAR(8000) NULL
		($5)::varchar, 	-- $5: CHANGED_BY VARCHAR(8000) NULL
		to_timestamp_ntz($6) 	-- $6: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/ConfigHistory/ConfigHistory_Backfill.csv.gz*';

-- // TABLE 9: ContactType
COPY INTO STG.CREO_ContactType_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::smallint, 	-- $1: CONTACT_TYPE_KEY SMALLINT NOT NULL
		($2)::varchar 	-- $2: DESCRIPTION VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/ContactType/ContactType_Backfill.csv.gz*';

-- // TABLE 10: Container
COPY INTO STG.CREO_Container_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: CONTAINER_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		to_timestamp_ntz($3), 	-- $3: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($4)::boolean, 	-- $4: IS_DELETED BOOLEAN NOT NULL
		($5)::boolean 	-- $5: IS_DEPRECATED BOOLEAN NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Container/Container_Backfill.csv.gz*';

-- // TABLE 11: Dataset
COPY INTO STG.CREO_Dataset_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: DATASET_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		to_timestamp_ntz($3), 	-- $3: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($4)::boolean, 	-- $4: IS_DELETED BOOLEAN NOT NULL
		($5)::int, 	-- $5: CONTAINER_KEY INT NOT NULL
		($6)::int, 	-- $6: ATTRIBUTES INT NOT NULL
		($7)::int, 	-- $7: ROW_COUNT INT NULL
		to_timestamp_ntz($8) 	-- $8: ROW_COUNT_UPDATED TIMESTAMP_LTZ NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Dataset/Dataset_Backfill.csv.gz*';

-- // TABLE 12: DatasetColumn
COPY INTO STG.CREO_DatasetColumn_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: DATASET_COLUMN_KEY INT NOT NULL
		($2)::varchar 	-- $2: NAME VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/DatasetColumn/DatasetColumn_Backfill.csv.gz*';

-- // TABLE 13: Datasource
COPY INTO STG.CREO_Datasource_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: DATASOURCE_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		($3)::varchar, 	-- $3: DEFINITION VARCHAR(8000) NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Datasource/Datasource_Backfill.csv.gz*';

-- // TABLE 14: DeadMessages
COPY INTO STG.CREO_DeadMessages_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: SUBJECT VARCHAR(8000) NULL
		to_timestamp_ntz($3), 	-- $3: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_SENT TIMESTAMP_LTZ NULL
		($5)::varchar, 	-- $5: EXCEPTION VARCHAR(8000) NULL
		($6)::int, 	-- $6: CONTAINER_KEY INT NULL
		($7)::int, 	-- $7: COMMUNICATION_MAILING_KEY INT NULL
		($8)::int, 	-- $8: TEMPLATE_KEY INT NULL
		($9)::boolean, 	-- $9: IS_PRODUCTION BOOLEAN NOT NULL
		($10)::bigint, 	-- $10: DATASET_ROW_KEY BIGINT NULL
		($11)::varchar, 	-- $11: SERVER VARCHAR(8000) NULL
		($12)::varchar, 	-- $12: MESSAGE_ID VARCHAR(36) NOT NULL
		($13)::smallint, 	-- $13: TYPE SMALLINT NULL
		($14)::int, 	-- $14: NUM_EXCEPTIONS INT NOT NULL
		($15)::int, 	-- $15: DELIVERY_STATUS_KEY INT NULL
		($16)::bigint, 	-- $16: IN_REPLY_TO_MESSAGE_KEY BIGINT NULL
		($17)::smallint, 	-- $17: DIRECTION SMALLINT NOT NULL
		to_timestamp_ntz($18), 	-- $18: SEND_AFTER TIMESTAMP_LTZ NULL
		($19)::bigint, 	-- $19: SEND_AFTER_MESSAGE_KEY BIGINT NULL
		($20)::boolean 	-- $20: IS_FINISHED BOOLEAN NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/DeadMessages/DeadMessages_Backfill.csv.gz*';

-- // TABLE 15: DeadMessages2
COPY INTO STG.CREO_DeadMessages2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: SUBJECT VARCHAR(8000) NULL
		to_timestamp_ntz($3), 	-- $3: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_SENT TIMESTAMP_LTZ NULL
		($5)::varchar, 	-- $5: EXCEPTION VARCHAR(8000) NULL
		($6)::int, 	-- $6: CONTAINER_KEY INT NULL
		($7)::int, 	-- $7: COMMUNICATION_MAILING_KEY INT NULL
		($8)::int, 	-- $8: TEMPLATE_KEY INT NULL
		($9)::boolean, 	-- $9: IS_PRODUCTION BOOLEAN NOT NULL
		($10)::bigint, 	-- $10: DATASET_ROW_KEY BIGINT NULL
		($11)::varchar, 	-- $11: SERVER VARCHAR(8000) NULL
		($12)::varchar, 	-- $12: MESSAGE_ID VARCHAR(36) NOT NULL
		($13)::smallint, 	-- $13: TYPE SMALLINT NULL
		($14)::int, 	-- $14: NUM_EXCEPTIONS INT NOT NULL
		($15)::int, 	-- $15: DELIVERY_STATUS_KEY INT NULL
		($16)::bigint, 	-- $16: IN_REPLY_TO_MESSAGE_KEY BIGINT NULL
		($17)::smallint, 	-- $17: DIRECTION SMALLINT NOT NULL
		to_timestamp_ntz($18), 	-- $18: SEND_AFTER TIMESTAMP_LTZ NULL
		($19)::bigint, 	-- $19: SEND_AFTER_MESSAGE_KEY BIGINT NULL
		($20)::boolean 	-- $20: IS_FINISHED BOOLEAN NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/DeadMessages2/DeadMessages2_Backfill.csv.gz*';

-- // TABLE 16: DeliveryStatus
COPY INTO STG.CREO_DeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: DELIVERY_STATUS_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		($3)::varchar, 	-- $3: REFERENCE VARCHAR(8000) NOT NULL
		($4)::int, 	-- $4: LIMIT_PER_MESSAGE INT NULL
		($5)::boolean, 	-- $5: IS_ERROR BOOLEAN NOT NULL
		($6)::boolean, 	-- $6: IS_FINAL BOOLEAN NOT NULL
		($7)::varchar 	-- $7: DESCRIPTION VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/DeliveryStatus/DeliveryStatus_Backfill.csv.gz*';

-- // TABLE 17: Emoji
COPY INTO STG.CREO_Emoji_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: EMOJI_KEY INT NOT NULL
		($2)::varchar, 	-- $2: SHORT_NAME VARCHAR(8000) NULL
		($3)::varchar, 	-- $3: DESCRIPTION VARCHAR(8000) NULL
		($4)::varchar 	-- $4: CODE VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Emoji/Emoji_Backfill.csv.gz*';

-- // TABLE 18: Folder
COPY INTO STG.CREO_Folder_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: FOLDER_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NULL
		($3)::int, 	-- $3: CONTAINER_KEY INT NOT NULL
		($4)::int, 	-- $4: PARENT_FOLDER_KEY INT NULL
		($5)::boolean, 	-- $5: IS_CATCH_ALL BOOLEAN NOT NULL
		($6)::int 	-- $6: DELETE_AFTER_DAYS INT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Folder/Folder_Backfill.csv.gz*';

-- // TABLE 19: FolderContact
COPY INTO STG.CREO_FolderContact_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: CONTACT_KEY INT NOT NULL
		($2)::int 	-- $2: FOLDER_KEY INT NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/FolderContact/FolderContact_Backfill.csv.gz*';

-- // TABLE 20: FolderMessage
COPY INTO STG.CREO_FolderMessage_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: FOLDER_KEY INT NOT NULL
		($3)::varchar, 	-- $3: LOCKED_BY VARCHAR(8000) NULL
		($4)::boolean 	-- $4: IS_READ BOOLEAN NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/FolderMessage/FolderMessage_Backfill.csv.gz*';

-- // TABLE 21: Global
COPY INTO STG.CREO_Global_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::decimal 	-- $1: APP_VERSION DECIMAL(38,0) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Global/Global_Backfill.csv.gz*';

-- // TABLE 22: Log
COPY INTO STG.CREO_Log_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::bigint, 	-- $1: LOG_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: ACTION VARCHAR(8000) NULL
		($3)::varchar, 	-- $3: DESCRIPTION VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: IP_ADDRESS VARCHAR(8000) NULL
		($5)::varchar, 	-- $5: USERNAME VARCHAR(8000) NULL
		($6)::int, 	-- $6: RULE_KEY INT NULL
		($7)::int, 	-- $7: TEMPLATE_KEY INT NULL
		($8)::int, 	-- $8: PACKAGE_KEY INT NULL
		($9)::int, 	-- $9: CONTAINER_KEY INT NULL
		($10)::bigint, 	-- $10: MESSAGE_KEY BIGINT NULL
		($11)::int, 	-- $11: CAMPAIGN_KEY INT NULL
		($12)::int, 	-- $12: COMMUNICATION_KEY INT NULL
		to_timestamp_ntz($13), 	-- $13: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($14)::varchar 	-- $14: SERVER VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Log/Log_Backfill.csv.gz*';

-- // TABLE 23: MessageContact
COPY INTO STG.CREO_MessageContact_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: MESSAGE_CONTACT_KEY INT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: CONTACT_KEY INT NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/MessageContact/MessageContact_Backfill.csv.gz*';

-- // TABLE 24: MessageContactType
COPY INTO STG.CREO_MessageContactType_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: MESSAGE_CONTACT_TYPE_KEY INT NOT NULL
		($2)::varchar 	-- $2: DESCRIPTION VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/MessageContactType/MessageContactType_Backfill.csv.gz*';

-- // TABLE 25: MessagePart
COPY INTO STG.CREO_MessagePart_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: MESSAGE_PART_KEY INT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
		($5)::variant, 	-- $5: DATA VARIANT NULL
		($6)::boolean 	-- $6: IS_COMPRESSED BOOLEAN NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/MessagePart/MessagePart_Backfill.csv.gz*';

-- // TABLE 26: MessageStatusQueue
COPY INTO STG.CREO_MessageStatusQueue_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_STATUS_QUEUE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: DATA VARCHAR(8000) NOT NULL
		($3)::varchar, 	-- $3: DETAIL VARCHAR(8000) NOT NULL
		($4)::int, 	-- $4: TYPE INT NOT NULL
		($5)::datetime, 	-- $5: ENTERED_AT datetime NOT NULL
		($6)::int, 	-- $6: CONTAINER_KEY INT NULL
		($7)::varchar 	-- $7: SERVER VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/MessageStatusQueue/MessageStatusQueue_Backfill.csv.gz*';

-- // TABLE 27: MessageType
COPY INTO STG.CREO_MessageType_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::smallint, 	-- $1: MESSAGE_TYPE_KEY SMALLINT NOT NULL
		($2)::varchar 	-- $2: DESCRIPTION VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/MessageType/MessageType_Backfill.csv.gz*';

-- // TABLE 28: Package
COPY INTO STG.CREO_Package_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: PACKAGE_KEY INT NOT NULL
		($2)::int, 	-- $2: CONTAINER_KEY INT NOT NULL
		to_timestamp_ntz($3), 	-- $3: DATE_PUBLISHED TIMESTAMP_LTZ NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::varchar, 	-- $5: HASH VARCHAR(8000) NULL
		($6)::varchar, 	-- $6: COMMIT_MESSAGE VARCHAR(8000) NULL
		($7)::varchar 	-- $7: PUBLISHED_BY VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Package/Package_Backfill.csv.gz*';

-- // TABLE 29: Parameter
COPY INTO STG.CREO_Parameter_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: PARAMETER_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		($3)::varchar, 	-- $3: DEFINITION VARCHAR(8000) NOT NULL
		($4)::int 	-- $4: DATASOURCE_KEY INT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Parameter/Parameter_Backfill.csv.gz*';

-- // TABLE 30: Rule
COPY INTO STG.CREO_Rule_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: RULE_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NOT NULL
		($3)::varchar, 	-- $3: PARAMETER_NAME VARCHAR(8000) NULL
		($4)::int, 	-- $4: TYPE INT NOT NULL
		to_timestamp_ntz($5), 	-- $5: DATE_START TIMESTAMP_LTZ NULL
		to_timestamp_ntz($6), 	-- $6: DATE_END TIMESTAMP_LTZ NULL
		($7)::boolean, 	-- $7: ENABLED BOOLEAN NOT NULL
		($8)::boolean, 	-- $8: IS_DELETED BOOLEAN NOT NULL
		to_timestamp_ntz($9), 	-- $9: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($10)::varchar, 	-- $10: DEFINITION VARCHAR(8000) NULL
		($11)::int, 	-- $11: WEIGHT INT NULL
		($12)::int 	-- $12: CONTAINER_KEY INT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Rule/Rule_Backfill.csv.gz*';

-- // TABLE 31: Template
COPY INTO STG.CREO_Template_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: TEMPLATE_KEY INT NOT NULL
		($2)::varchar, 	-- $2: PATH VARCHAR(8000) NOT NULL
		($3)::variant, 	-- $3: CONTENT VARIANT NULL
		($4)::smallint, 	-- $4: TYPE SMALLINT NOT NULL
		to_timestamp_ntz($5), 	-- $5: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($6)::varchar, 	-- $6: DESCRIPTION VARCHAR(8000) NULL
		($7)::int, 	-- $7: BASE_TEMPLATE_KEY INT NULL
		($8)::varchar, 	-- $8: CONTENT_HASH VARCHAR(8000) NULL
		($9)::varchar, 	-- $9: NOTES VARCHAR(8000) NULL
		($10)::varchar 	-- $10: EDITED_BY VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/Template/Template_Backfill.csv.gz*';

-- // TABLE 32: TemplateRule
COPY INTO STG.CREO_TemplateRule_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: TEMPLATE_KEY INT NOT NULL
		($2)::int 	-- $2: RULE_KEY INT NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/TemplateRule/TemplateRule_Backfill.csv.gz*';

-- // TABLE 33: TemplateType
COPY INTO STG.CREO_TemplateType_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::smallint, 	-- $1: TEMPLATE_TYPE_KEY SMALLINT NOT NULL
		($2)::varchar 	-- $2: DESCRIPTION VARCHAR(8000) NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/TemplateType/TemplateType_Backfill.csv.gz*';

-- // TABLE 34: TempMessage
COPY INTO STG.CREO_TempMessage_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: TEMP_MESSAGE_KEY INT NOT NULL
		($2)::varchar 	-- $2: Json VARCHAR(8000) NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/TempMessage/TempMessage_Backfill.csv.gz*';

-- // TABLE 35: User
COPY INTO STG.CREO_User_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: USER_KEY INT NOT NULL
		($2)::varchar, 	-- $2: USERNAME VARCHAR(8000) NOT NULL
		($3)::varchar, 	-- $3: NAME VARCHAR(8000) NOT NULL
		to_timestamp_ntz($4) 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/User/User_Backfill.csv.gz*';

-- // TABLE 36: WebHook
COPY INTO STG.CREO_WebHook_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-03'), 
        ($1)::int, 	-- $1: WEBHOOK_KEY INT NOT NULL
		($2)::int, 	-- $2: CONTAINER_KEY INT NOT NULL
		($3)::int, 	-- $3: TYPE INT NOT NULL
		($4)::varchar, 	-- $4: URL VARCHAR(8000) NOT NULL
		($5)::varchar, 	-- $5: EVENT_NAME VARCHAR(8000) NULL
		to_timestamp_ntz($6) 	-- $6: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
    FROM @ETL.INBOUND
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = 'inbound/CREO/Backfill/WebHook/WebHook_Backfill.csv.gz*';