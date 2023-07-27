USE SCHEMA {{ SF_DATABASE }}.STG;

/*************************************************************************/
/* Note:																 */ 
/* 1. MUST USE 'CREATE TABLE IF NOT EXISTS'                              */
/* 2. DO NOT USE MASKING POLICIES - They will be applied separately      */
/*************************************************************************/

-- // TABLE 41: Contact 
CREATE TABLE IF NOT EXISTS STG.CREO_CONTACT_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    CONTACT_KEY INT NOT NULL,
	NAME VARCHAR(8000) NULL COLLATE 'en-ci',
	ADDRESS VARCHAR(8000) NULL COLLATE 'en-ci',
	DATE_ENTERED TIMESTAMP_LTZ NOT NULL,
	CONTACT_TYPE SMALLINT NOT NULL,
	IS_EXPIRED BOOLEAN NOT NULL,
	NUM_ERRORS INT NOT NULL,
	IS_INVALID BOOLEAN NOT NULL 
);

-- // TABLE 42: Message 
CREATE TABLE IF NOT EXISTS STG.CREO_MESSAGE_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    MESSAGE_KEY BIGINT NOT NULL,
	SUBJECT VARCHAR(8000) NULL COLLATE 'en-ci',
	DATE_ENTERED TIMESTAMP_LTZ NOT NULL,
	DATE_SENT TIMESTAMP_LTZ NULL,
	EXCEPTION VARCHAR NULL COLLATE 'en-ci',
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
	HEADERS VARCHAR NULL COLLATE 'en-ci',
	VENDOR_ID VARCHAR(8000) NULL COLLATE 'en-ci'
);

-- // TABLE 43: MessageDeliveryStatus 
CREATE TABLE IF NOT EXISTS STG.CREO_MESSAGEDELIVERYSTATUS_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    MESSAGE_DELIVERY_STATUS_KEY BIGINT NOT NULL,
	MESSAGE_KEY BIGINT NOT NULL,
	DELIVERY_STATUS_KEY INT NOT NULL,
	DATE_ENTERED TIMESTAMP_LTZ NOT NULL,
	DETAIL VARCHAR NULL COLLATE 'en-ci'
);

-- // TABLE 44: MessagePartV2 
CREATE TABLE IF NOT EXISTS STG.CREO_MESSAGEPARTV2_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    MESSAGE_PART_KEY BIGINT NOT NULL,
	MESSAGE_KEY BIGINT NULL,
	CONTENT_TYPE VARCHAR(8000) NULL COLLATE 'en-ci',
	FILENAME VARCHAR(8000) NULL COLLATE 'en-ci',
	DATA VARBINARY NULL 
);
