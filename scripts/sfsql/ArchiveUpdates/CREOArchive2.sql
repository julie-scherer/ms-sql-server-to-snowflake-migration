-- https://github.com/CuroFinTechCorp/Curo-Astro/blob/AB%23204967-CREOArchive2-Backfill/include/sql/ImplementationScripts/20230830_AB%23204967_CREO_Archive2_Fix.sql

-- // TABLE 3: Message
DELETE FROM STG.CREO_Message_HIST WHERE ASOFDATE = to_date('2023-08-20');
COPY INTO STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-30'), 
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

-- // TABLE 5: MessageDeliveryStatus
DELETE FROM STG.CREO_MessageDeliveryStatus_HIST WHERE ASOFDATE = to_date('2023-08-18');
COPY INTO STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-30'), 
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
