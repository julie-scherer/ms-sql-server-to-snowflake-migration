
-- // TABLE 41: Contact
COPY INTO STG.CREO_Contact_HIST FROM 
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-28'),
    $1,
    $2,
    $3,
    to_timestamp($4, 'MM/DD/YYYY HH12:MI:SS AM TZH:TZM'),
    $5,
    $6,
    $7,
    $8

FROM @ETL.INBOUND/CREO/Backfill/Contact/)
FILE_FORMAT = ( FORMAT_NAME = ARES.STG.LD_CSV_PIPE_SH1_EON_GZ)
PATTERN = '.*Contact_Backfill_[0-9]+\.csv\.gz';


-- // TABLE 42: MessageDeliveryStatus
COPY INTO STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-27'), 
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
    REPLACE_INVALID_CHARACTERS = TRUE -- Additional field added to this table to replace invalid UTF-8 characters
)
PATTERN = '.*MessageDeliveryStatus_Backfill_[0-9]+\.csv\.gz';
