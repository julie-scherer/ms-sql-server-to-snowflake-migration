
-- // TABLE 43: MessagePartV2
COPY INTO ARES.STG.CREO_MessagePartV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: MESSAGE_PART_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
		($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
		($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
		($5)::varchar 	-- $5: DATA VARCHAR NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessagePartV2/
    -- FROM @DEV_JS.STG.TEST_STAGE/CREO/Backfill/MessagePartV2/
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
LIST @ETL.INBOUND/CREO/Backfill/MessagePartV2/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessagePartV2_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessagePartV2_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_MessagePartV2_HIST; -- preview data
*/

