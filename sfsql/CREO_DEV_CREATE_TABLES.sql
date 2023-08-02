
-- // TABLE 45: DatasetValue 
-- DROP TABLE IF EXISTS STG.CREO_DATASETVALUE_HIST;
CREATE TABLE IF NOT EXISTS STG.CREO_DATASETVALUE_HIST ( 
    METADATAFILENAME VARCHAR(16777216) NOT NULL COLLATE 'en-ci', LOADTIMESTAMP TIMESTAMP_NTZ(9) NOT NULL, ASOFDATE DATE,
    DATASET_VALUE_KEY BIGINT NOT NULL,
	VALUE VARCHAR(8000) NOT NULL COLLATE 'en-ci',
	VALUE_HASH VARCHAR NULL 
);
