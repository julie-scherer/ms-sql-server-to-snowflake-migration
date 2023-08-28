
-- // TABLE 1: DatasetCell 
-- >> NO CHANGES
COPY INTO STG.CREO_DatasetCell_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-17'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_COLUMN_KEY INT NOT NULL
		($3)::bigint 	-- $3: DATASET_VALUE_KEY BIGINT NOT NULL
    FROM @ETL.INBOUND/CREOArchive2/Backfill/DatasetCell/
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

-- // TABLE 2: DatasetRow
-- >> CHECK IF UPDATED
COPY INTO STG.CREO_DatasetRow_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-17'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_KEY INT NOT NULL
		($3)::boolean 	-- $3: HAS_MESSAGE BOOLEAN NOT NULL
    FROM @ETL.INBOUND/CREOArchive2/Backfill/DatasetRow/
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



-- // TABLE 3: Message -- UPDATED
DELETE FROM STG.CREO_Message_HIST WHERE ASOFDATE = to_date('2023-08-20'); -- 

COPY INTO STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-28'), 
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
    FROM @ETL.INBOUND/CREOArchive2/Backfill/Message/
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

SELECT COUNT(*) FROM STG.CREO_Message_HIST WHERE ASOFDATE = to_date('2023-08-28'); -- 


-- // TABLE 4: MessageContactV2
-- >> UPDATED
DELETE FROM STG.CREO_MessageContactV2_HIST WHERE ASOFDATE = to_date('2023-08-17'); -- 

COPY INTO STG.CREO_MessageContactV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-28'), 
        ($1)::bigint, 	-- $1: MESSAGE_CONTACT_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: CONTACT_KEY INT NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    FROM @ETL.INBOUND/CREOArchive2/Backfill/MessageContactV2/
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

SELECT COUNT(*) FROM STG.CREO_MessageContactV2_HIST WHERE ASOFDATE = to_date('2023-08-18'); -- 


-- // TABLE 5: MessageDeliveryStatus 
-- >> UPDATED 8/28
DELETE FROM STG.CREO_MessageDeliveryStatus_HIST WHERE ASOFDATE = to_date('2023-08-18');

COPY INTO STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-28'), 
        ($1)::bigint, 	-- $1: MESSAGE_DELIVERY_STATUS_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: DELIVERY_STATUS_KEY INT NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::varchar 	-- $5: DETAIL VARCHAR NULL
    FROM @DEV_JS.STG.TEST_STAGE/CREOArchive2/Backfill/MessageDeliveryStatus
    -- FROM @ETL.INBOUND/CREOArchive2/Backfill/MessageDeliveryStatus/
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

SELECT COUNT(*) FROM STG.CREO_MessageDeliveryStatus_HIST WHERE ASOFDATE = to_date('2023-08-28'); -- 



-- // TABLE 6: MessagePartV2
COPY INTO STG.CREO_MessagePartV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-24'), 
        ($1)::bigint, 	-- $1: MESSAGE_PART_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
		($5)::varchar 	-- $5: DATA VARCHAR NULL
    FROM @ETL.INBOUND/CREOArchive2/Backfill/MessagePartV2/
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