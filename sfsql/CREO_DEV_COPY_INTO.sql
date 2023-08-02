
-- // TABLE 45: DatasetValue
COPY INTO ARES.STG.CREO_DatasetValue_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-03'), 
        ($1)::bigint, 	-- $1: DATASET_VALUE_KEY BIGINT NOT NULL
		($2)::varchar, 	-- $2: VALUE VARCHAR(8000) NOT NULL
		($3)::varchar 	-- $3: VALUE_HASH VARCHAR NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetValue/
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
PATTERN = '.*DatasetValue_Backfill_[0-9]+\.csv\.gz';
/*
-- // RUN STATUS >> [tbd]

TRUNCATE TABLE IF EXISTS STG.CREO_DATASETVALUE_HIST; -- drop records
LIST @ETL.INBOUND/CREO/Backfill/DatasetValue/; -- list files in S3
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetValue_HIST',3); -- get columns in $n format

SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetValue_HIST; -- check row count
SELECT TOP 10 * FROM ARES.STG.CREO_DatasetValue_HIST; -- preview data
*/

