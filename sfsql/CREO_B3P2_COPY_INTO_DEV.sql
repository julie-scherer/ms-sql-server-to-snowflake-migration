
-- // TABLE 43: Message
COPY INTO ARES.STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-02'), 
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
    FROM @ETL.INBOUND/CREO/Backfill/Message/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    REPLACE_INVALID_CHARACTERS = TRUE
    NULL_IF = 'NULL'

)
PATTERN = '.*Message_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGE_HIST; -- drop records
LIST @ETL.INBOUND/CREO/Backfill/Message/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_Message_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_Message_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_Message_HIST; -- preview data
*/


-- // TABLE 44: MessagePartV2
COPY INTO ARES.STG.CREO_MessagePartV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-02'), 
        ($1)::bigint, 	-- $1: MESSAGE_PART_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
		($5)::varbinary 	-- $5: DATA VARBINARY NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessagePartV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    REPLACE_INVALID_CHARACTERS = TRUE
    NULL_IF = 'NULL'

)
PATTERN = '.*MessagePartV2_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_MESSAGEPARTV2_HIST; -- drop records
LIST @ETL.INBOUND/CREO/Backfill/MessagePartV2/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessagePartV2_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessagePartV2_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessagePartV2_HIST; -- preview data
*/

