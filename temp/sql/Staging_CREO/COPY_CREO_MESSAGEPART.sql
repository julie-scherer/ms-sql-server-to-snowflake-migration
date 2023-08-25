COPY INTO STG.CREO_MESSAGEPART_HIST 
FROM (
    SELECT 
    METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('asOfDate'), 
    ($1)::int, 	-- $1: MESSAGE_PART_KEY INT NOT NULL
    ($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NULL
    ($3)::varchar, 	-- $3: CONTENT_TYPE VARCHAR(8000) NULL
    ($4)::varchar, 	-- $4: FILENAME VARCHAR(8000) NULL
    ($5)::varchar, 	-- $5: DATA VARCHAR NULL
    ($6)::boolean 	-- $6: IS_COMPRESSED BOOLEAN NOT NULL 
    FROM @ETL.INBOUND/{s3_dir_path}/
)
FILE_FORMAT = ( 
    FORMAT_NAME = {file_format}
) 
PATTERN = '.*MESSAGEPART_{asOfDate}.*';