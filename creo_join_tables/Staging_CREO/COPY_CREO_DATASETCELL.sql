WITH 
    JoinRowDate AS (
        SELECT
            DATASET_ROW_KEY
            ,[DATE_ENTERED]
        FROM [dbo].[DATASETROW] dr
        LEFT JOIN [dbo].[DATASET] ds ON dr.DATASET_KEY = ds.DATASET_KEY
        WHERE CAST(DATE_ENTERED AS DATE) = CAST('{asOfDate}' AS DATE)
    )
    ,JoinCellRow AS (
        SELECT 
            dc.DATASET_VALUE_KEY, 
            dc.DATASET_ROW_KEY, 
            dc.DATASET_COLUMN_KEY,
            rd.DATE_ENTERED
        FROM [dbo].[DatasetCell] AS dc
        LEFT JOIN JoinRowDate rd ON dc.DATASET_ROW_KEY = rd.DATASET_ROW_KEY
    )
SELECT DATASET_VALUE_KEY, DATASET_ROW_KEY, DATASET_COLUMN_KEY 
FROM JoinCellRow;
