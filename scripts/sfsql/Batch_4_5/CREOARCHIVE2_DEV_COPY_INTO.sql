
-- // TABLE 1: DatasetCell
COPY INTO ARES.STG.CREO_DatasetCell_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_COLUMN_KEY INT NOT NULL
		($3)::bigint 	-- $3: DATASET_VALUE_KEY BIGINT NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/DatasetCell/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/DatasetCell/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*DatasetCell_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_DATASETCELL_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/DatasetCell/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetCell_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetCell_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_DatasetCell_HIST; -- preview data
*/


-- // TABLE 2: DatasetRow
COPY INTO ARES.STG.CREO_DatasetRow_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_KEY INT NOT NULL
		($3)::boolean 	-- $3: HAS_MESSAGE BOOLEAN NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/DatasetRow/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/DatasetRow/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*DatasetRow_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_DATASETROW_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/DatasetRow/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetRow_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetRow_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_DatasetRow_HIST; -- preview data
*/


-- // TABLE 3: Global
COPY INTO ARES.STG.CREO_Global_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::decimal 	-- $1: APP_VERSION DECIMAL(38,0) NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/Global/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/Global/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*Global_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_GLOBAL_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/Global/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_Global_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_Global_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_Global_HIST; -- preview data
*/


-- // TABLE 4: Message
COPY INTO ARES.STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: SUBJECT VARCHAR(8000) NULL
		to_timestamp_ntz($3), 	-- $3: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_SENT TIMESTAMP_LTZ NULL
		($5)::varchar, 	-- $5: EXCEPTION VARCHAR NULL
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
		($20)::boolean, 	-- $20: IS_FINISHED BOOLEAN NOT NULL
		($21)::varchar, 	-- $21: HEADERS VARCHAR NULL
		($22)::varchar 	-- $22: VENDOR_ID VARCHAR(8000) NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/Message/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/Message/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*Message_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGE_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/Message/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_Message_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_Message_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_Message_HIST; -- preview data
*/


-- // TABLE 5: MessageContactV2
COPY INTO ARES.STG.CREO_MessageContactV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_CONTACT_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: CONTACT_KEY INT NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/MessageContactV2/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/MessageContactV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessageContactV2_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGECONTACTV2_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/MessageContactV2/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessageContactV2_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessageContactV2_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessageContactV2_HIST; -- preview data
*/


-- // TABLE 6: MessageDeliveryStatus
COPY INTO ARES.STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_DELIVERY_STATUS_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: DELIVERY_STATUS_KEY INT NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::varchar 	-- $5: DETAIL VARCHAR NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/MessageDeliveryStatus/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/MessageDeliveryStatus/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessageDeliveryStatus_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGEDELIVERYSTATUS_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/MessageDeliveryStatus/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessageDeliveryStatus_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessageDeliveryStatus_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessageDeliveryStatus_HIST; -- preview data
*/


-- // TABLE 7: MessagePartV2
COPY INTO ARES.STG.CREO_MessagePartV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_PART_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
		($5)::varbinary 	-- $5: DATA VARBINARY NULL
    -- FROM @ETL.INBOUND/CREOArchive/Backfill/MessagePartV2/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive/Backfill/MessagePartV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessagePartV2_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGEPARTV2_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive/Backfill/MessagePartV2/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessagePartV2_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessagePartV2_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessagePartV2_HIST; -- preview data
*/


-- // TABLE 8: DatasetCell
COPY INTO ARES.STG.CREO_DatasetCell_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_COLUMN_KEY INT NOT NULL
		($3)::bigint 	-- $3: DATASET_VALUE_KEY BIGINT NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/DatasetCell/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/DatasetCell/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*DatasetCell_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_DATASETCELL_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive2/Backfill/DatasetCell/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetCell_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetCell_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_DatasetCell_HIST; -- preview data
*/


-- // TABLE 9: DatasetRow
COPY INTO ARES.STG.CREO_DatasetRow_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_KEY INT NOT NULL
		($3)::boolean 	-- $3: HAS_MESSAGE BOOLEAN NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/DatasetRow/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/DatasetRow/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*DatasetRow_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_DATASETROW_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive2/Backfill/DatasetRow/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetRow_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetRow_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_DatasetRow_HIST; -- preview data
*/


-- // TABLE 10: Message
COPY INTO ARES.STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: SUBJECT VARCHAR(8000) NULL
		to_timestamp_ntz($3), 	-- $3: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_SENT TIMESTAMP_LTZ NULL
		($5)::varchar, 	-- $5: EXCEPTION VARCHAR NULL
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
		($20)::boolean, 	-- $20: IS_FINISHED BOOLEAN NOT NULL
		($21)::varchar, 	-- $21: HEADERS VARCHAR NULL
		($22)::varchar 	-- $22: VENDOR_ID VARCHAR(8000) NULL
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/Message/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/Message/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*Message_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGE_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive2/Backfill/Message/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_Message_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_Message_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_Message_HIST; -- preview data
*/


-- // TABLE 11: MessageContactV2
COPY INTO ARES.STG.CREO_MessageContactV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_CONTACT_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: CONTACT_KEY INT NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/MessageContactV2/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/MessageContactV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessageContactV2_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGECONTACTV2_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive2/Backfill/MessageContactV2/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessageContactV2_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessageContactV2_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessageContactV2_HIST; -- preview data
*/


-- // TABLE 12: MessageDeliveryStatus
COPY INTO ARES.STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_DELIVERY_STATUS_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: DELIVERY_STATUS_KEY INT NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::varchar 	-- $5: DETAIL VARCHAR NULL
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/MessageDeliveryStatus/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/MessageDeliveryStatus/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessageDeliveryStatus_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGEDELIVERYSTATUS_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive2/Backfill/MessageDeliveryStatus/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessageDeliveryStatus_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessageDeliveryStatus_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessageDeliveryStatus_HIST; -- preview data
*/


-- // TABLE 13: MessagePartV2
COPY INTO ARES.STG.CREO_MessagePartV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_PART_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
		($5)::varbinary 	-- $5: DATA VARBINARY NULL
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/MessagePartV2/
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/MessagePartV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = 'NULL'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessagePartV2_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGEPARTV2_HIST; -- drop records
LIST @ETL.INBOUND/CREOArchive2/Backfill/MessagePartV2/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessagePartV2_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessagePartV2_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessagePartV2_HIST; -- preview data
*/

