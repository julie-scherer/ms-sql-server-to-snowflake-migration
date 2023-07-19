
-- // TABLE 37: Contact
COPY INTO ARES.STG.CREO_Contact_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
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
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*Contact_Backfill.csv.gz';
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_Contact_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_Contact_HIST;
SELECT * FROM ARES.STG.CREO_Contact_HIST;
[STATUS=tbd]
*/

-- // TABLE 38: DatasetCell
COPY INTO ARES.STG.CREO_DatasetCell_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_COLUMN_KEY INT NOT NULL
		($3)::bigint 	-- $3: DATASET_VALUE_KEY BIGINT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetCell/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*DatasetCell_Backfill.csv.gz';
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetCell_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetCell_HIST;
SELECT * FROM ARES.STG.CREO_DatasetCell_HIST;
[STATUS=tbd]
*/

-- // TABLE 39: DatasetRow
COPY INTO ARES.STG.CREO_DatasetRow_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_KEY INT NOT NULL
		($3)::boolean 	-- $3: HAS_MESSAGE BOOLEAN NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetRow/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*DatasetRow_Backfill.csv.gz';
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_DatasetRow_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_DatasetRow_HIST;
SELECT * FROM ARES.STG.CREO_DatasetRow_HIST;
[STATUS=tbd]
*/

-- // TABLE 40: Message
COPY INTO ARES.STG.CREO_Message_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
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
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*Message_Backfill.csv.gz';
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_Message_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_Message_HIST;
SELECT * FROM ARES.STG.CREO_Message_HIST;
[STATUS=tbd]
*/

-- // TABLE 41: MessageContactV2
COPY INTO ARES.STG.CREO_MessageContactV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
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
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessageContactV2_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessageContactV2_HIST;
SELECT * FROM ARES.STG.CREO_MessageContactV2_HIST;
[STATUS=tbd]
*/

-- // TABLE 42: MessageDeliveryStatus
COPY INTO ARES.STG.CREO_MessageDeliveryStatus_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
        ($1)::bigint, 	-- $1: MESSAGE_DELIVERY_STATUS_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: DELIVERY_STATUS_KEY INT NOT NULL
		to_timestamp_ntz($4), 	-- $4: DATE_ENTERED TIMESTAMP_LTZ NOT NULL
		($5)::varchar 	-- $5: DETAIL VARCHAR(8000) NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessageDeliveryStatus/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*MessageDeliveryStatus_Backfill.csv.gz';
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_MessageDeliveryStatus_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_MessageDeliveryStatus_HIST;
SELECT * FROM ARES.STG.CREO_MessageDeliveryStatus_HIST;
[STATUS=tbd]
*/

-- // TABLE 43: PackageTemplate
COPY INTO ARES.STG.CREO_PackageTemplate_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-18'), 
        ($1)::int, 	-- $1: PACKAGE_KEY INT NOT NULL
		($2)::int 	-- $2: TEMPLATE_KEY INT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/PackageTemplate/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
)
PATTERN = '.*PackageTemplate_Backfill.csv.gz';
/*
SELECT ARES.ETL.COPYSELECT('STG','CREO_PackageTemplate_HIST',3);
SELECT COUNT(*) AS row_count FROM ARES.STG.CREO_PackageTemplate_HIST;
SELECT * FROM ARES.STG.CREO_PackageTemplate_HIST;
[STATUS=tbd]
*/
