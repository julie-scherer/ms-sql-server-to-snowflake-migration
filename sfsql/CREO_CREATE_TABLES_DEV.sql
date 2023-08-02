
-- // TABLE 43: Message 
-- DROP TABLE IF EXISTS STG.CREO_MESSAGE_HIST;
CREATE TABLE IF NOT EXISTS STG.CREO_MESSAGE_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    MESSAGE_KEY BIGINT NOT NULL,
	SUBJECT VARCHAR(8000) NULL COLLATE 'en-ci',
	DATE_ENTERED TIMESTAMP_LTZ NOT NULL,
	DATE_SENT TIMESTAMP_LTZ NULL,
	EXCEPTION VARCHAR(8000) NULL COLLATE 'en-ci',
	CONTAINER_KEY INT NULL,
	COMMUNICATION_MAILING_KEY INT NULL,
	TEMPLATE_KEY INT NULL,
	IS_PRODUCTION BOOLEAN NOT NULL,
	DATASET_ROW_KEY BIGINT NULL,
	SERVER VARCHAR(8000) NULL COLLATE 'en-ci',
	MESSAGE_ID VARCHAR(36) NOT NULL,
	TYPE SMALLINT NULL,
	NUM_EXCEPTIONS INT NOT NULL,
	DELIVERY_STATUS_KEY INT NULL,
	IN_REPLY_TO_MESSAGE_KEY BIGINT NULL,
	DIRECTION SMALLINT NOT NULL,
	SEND_AFTER TIMESTAMP_LTZ NULL,
	SEND_AFTER_MESSAGE_KEY BIGINT NULL,
	IS_FINISHED BOOLEAN NOT NULL,
	HEADERS VARCHAR(8000) NULL COLLATE 'en-ci',
	VENDOR_ID VARCHAR(8000) NULL COLLATE 'en-ci'
);

-- // TABLE 44: MessagePartV2 
-- DROP TABLE IF EXISTS STG.CREO_MESSAGEPARTV2_HIST;
CREATE TABLE IF NOT EXISTS STG.CREO_MESSAGEPARTV2_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    MESSAGE_PART_KEY BIGINT NOT NULL,
	MESSAGE_KEY BIGINT NULL,
	CONTENT_TYPE VARCHAR(8000) NULL COLLATE 'en-ci',
	FILENAME VARCHAR(8000) NULL COLLATE 'en-ci',
	DATA VARBINARY NULL 
);
