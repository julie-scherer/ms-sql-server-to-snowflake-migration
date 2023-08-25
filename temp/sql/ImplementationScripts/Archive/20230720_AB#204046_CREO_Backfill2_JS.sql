
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
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*DatasetCell_Backfill.csv.gz';

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
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*DatasetRow_Backfill.csv.gz';

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
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*MessageContactV2_Backfill.csv.gz';

-- // TABLE 40: PackageTemplate
COPY INTO STG.CREO_PackageTemplate_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-20'), 
        ($1)::int, 	-- $1: PACKAGE_KEY INT NOT NULL
		($2)::int 	-- $2: TEMPLATE_KEY INT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/PackageTemplate/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*PackageTemplate_Backfill.csv.gz';
