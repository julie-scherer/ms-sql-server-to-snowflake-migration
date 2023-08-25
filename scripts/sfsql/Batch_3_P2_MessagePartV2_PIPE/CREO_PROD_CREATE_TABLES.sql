USE SCHEMA {{ SF_DATABASE }}.STG;

/*************************************************************************/
/* Note:																 */ 
/* 1. MUST USE 'CREATE TABLE IF NOT EXISTS'                              */
/* 2. DO NOT USE MASKING POLICIES - They will be applied separately      */
/*************************************************************************/

-- // TABLE 43: MessagePartV2 
CREATE TABLE IF NOT EXISTS STG.CREO_MESSAGEPARTV2_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    MESSAGE_PART_KEY BIGINT NOT NULL,
	MESSAGE_KEY BIGINT NULL,
	CONTENT_TYPE VARCHAR(8000) NULL COLLATE 'en-ci',
	FILENAME VARCHAR(8000) NULL COLLATE 'en-ci',
	DATA VARCHAR NULL 
);
