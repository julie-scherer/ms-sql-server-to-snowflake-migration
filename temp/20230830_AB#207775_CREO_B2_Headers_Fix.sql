
-- // TABLE 37: DatasetCell
COPY INTO STG.CREO_DatasetCell_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-20'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_COLUMN_KEY INT NOT NULL
		($3)::bigint 	-- $3: DATASET_VALUE_KEY BIGINT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetCell/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = '\\N'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*DatasetCell_Backfill_0.csv';


-- // TABLE 38: DatasetRow
COPY INTO STG.CREO_DatasetRow_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-20'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_KEY INT NOT NULL
		($3)::boolean 	-- $3: HAS_MESSAGE BOOLEAN NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetRow/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = '\\N'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*DatasetRow_Backfill_0.csv';


-- // TABLE 39: MessageContactV2
COPY INTO STG.CREO_MessageContactV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-20'), 
        ($1)::bigint, 	-- $1: MESSAGE_CONTACT_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: CONTACT_KEY INT NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessageContactV2/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = '\\N'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*MessageContactV2_Backfill_0.csv';


-- // TABLE 40: PackageTemplate
COPY INTO STG.CREO_PackageTemplate_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-20'), 
        ($1)::int, 	-- $1: PACKAGE_KEY INT NOT NULL
		($2)::int 	-- $2: TEMPLATE_KEY INT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/PackageTemplate/
)
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = NONE
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    NULL_IF = '\\N'
    REPLACE_INVALID_CHARACTERS = TRUE
)
PATTERN = '.*PackageTemplate_Backfill_0.csv';

