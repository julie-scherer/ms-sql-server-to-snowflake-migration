
-- // TABLE 41: Contact
COPY INTO STG.CREO_Contact_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-26'), 
        ($1)::int, 	-- $1: CONTACT_KEY INT NOT NULL
		($2)::varchar, 	-- $2: NAME VARCHAR(8000) NULL
		($3)::varchar, 	-- $3: ADDRESS VARCHAR(8000) NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::smallint, 	-- $5: CONTACT_TYPE SMALLINT NOT NULL
		($6)::boolean, 	-- $6: IS_EXPIRED BOOLEAN NOT NULL
		($7)::int, 	-- $7: NUM_ERRORS INT NOT NULL
		($8)::boolean 	-- $8: IS_INVALID BOOLEAN NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/Contact/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    EMPTY_FIELD_AS_NULL = TRUE 
)
PATTERN = '.*Contact_Backfill_[0-9]+\.csv\.gz';

-- // TABLE 42: DatasetValue
COPY INTO STG.CREO_DatasetValue_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-26'), 
        ($1)::bigint, 	-- $1: DATASET_VALUE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: VALUE VARCHAR(8000) NOT NULL
		($3)::binary 	-- $3: VALUE_HASH BINARY NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetValue/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    EMPTY_FIELD_AS_NULL = TRUE 
)
PATTERN = '.*DatasetValue_Backfill_[0-9]+\.csv\.gz';

-- // TABLE 43: Message
COPY INTO STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-26'), 
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
		($20)::boolean, 	-- $20: IS_FINISHED BOOLEAN NOT NULL
		($21)::varchar, 	-- $21: HEADERS VARCHAR(8000) NULL
		($22)::varchar 	-- $22: VENDOR_ID VARCHAR(8000) NULL
    FROM @ETL.INBOUND/CREO/Backfill/Message/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    EMPTY_FIELD_AS_NULL = TRUE 
)
PATTERN = '.*Message_Backfill_[0-9]+\.csv\.gz';

-- // TABLE 44: MessageDeliveryStatus
COPY INTO STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-26'), 
        ($1)::bigint, 	-- $1: MESSAGE_DELIVERY_STATUS_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: DELIVERY_STATUS_KEY INT NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::varchar 	-- $5: DETAIL VARCHAR(8000) NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    EMPTY_FIELD_AS_NULL = TRUE 
)
PATTERN = '.*MessageDeliveryStatus_Backfill_[0-9]+\.csv\.gz';

-- // TABLE 45: MessagePartV2
COPY INTO STG.CREO_MessagePartV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-26'), 
        ($1)::bigint, 	-- $1: MESSAGE_PART_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
        NULL
		-- ($5)::varbinary 	-- $5: DATA VARBINARY NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessagePartV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '^'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    EMPTY_FIELD_AS_NULL = TRUE 
)
PATTERN = '.*MessagePartV2_Backfill_[0-9]+\.csv\.gz';
