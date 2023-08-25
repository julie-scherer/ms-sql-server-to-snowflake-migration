COPY INTO STG.CREO_TEMPLATE_HIST 
FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('{previous_day}'),
        ($1)::int, 	-- $1: TEMPLATE_KEY INT NOT NULL
        ($2)::varchar, 	-- $2: PATH VARCHAR(8000) NOT NULL
        ($3)::varchar, 	-- $3: CONTENT VARCHAR NULL
        ($4)::smallint, 	-- $4: TYPE SMALLINT NOT NULL
        to_timestamp_ntz($5), 	-- $5: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
        ($6)::varchar, 	-- $6: DESCRIPTION VARCHAR(8000) NULL
        ($7)::int, 	-- $7: BASE_TEMPLATE_KEY INT NULL
        ($8)::varchar, 	-- $8: CONTENT_HASH VARCHAR(8000) NULL
        ($9)::varchar, 	-- $9: NOTES VARCHAR(8000) NULL
        ($10)::varchar 	-- $10: EDITED_BY VARCHAR(8000) NULL 
    FROM @ETL.INBOUND/{s3_dir_path}/
)
FILE_FORMAT = ( 
    FORMAT_NAME = {file_format}
) 
PATTERN = '.*{file_name}.*'