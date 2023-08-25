
-- ** DATASETCELL **
-- 836,196,991
SELECT COUNT(*) 
FROM ZEUS.STG.CREO_DATASETCELL_HIST
WHERE ASOFDATE = TO_DATE('2023-07-20');

WITH 
    JoinRowDate AS (
        SELECT
            DATASET_ROW_KEY
            ,DATE_ENTERED
        FROM ZEUS.STG.CREO_DATASETROW_HIST dr
        LEFT JOIN ZEUS.STG.CREO_DATASET_HIST ds
        ON dr.DATASET_KEY = ds.DATASET_KEY
        WHERE TO_DATE(DATE_ENTERED) = TO_DATE('2023-07-16 00:00:00')
    )
    ,JoinCellRow AS (
        SELECT 
            dc.DATASET_VALUE_KEY, 
            dc.DATASET_ROW_KEY, 
            dc.DATASET_COLUMN_KEY,
            rd.DATE_ENTERED
        FROM ZEUS.STG.CREO_DatasetCell_HIST AS dc
        LEFT JOIN JoinRowDate rd
        ON dc.DATASET_ROW_KEY = rd.DATASET_ROW_KEY
    )
SELECT COUNT(*)
-- DATASET_VALUE_KEY, DATASET_ROW_KEY, DATASET_COLUMN_KEY 
FROM JoinCellRow;



-- ** PACKAGETEMPLATE **
-- 2,183,884
SELECT COUNT(*) 
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
WHERE ASOFDATE = TO_DATE('2023-07-20');


-- * * * * * * * * SMALLER TABLES * * * * * * * * * 

-- ** FOLDERCONTACT **
-- 122
SELECT COUNT(*) 
FROM ZEUS.STG.CREO_FOLDERCONTACT_HIST
WHERE ASOFDATE = TO_DATE('2023-07-03');

-- ** FOLDERMESSAGE **
SELECT COUNT(*) 
FROM ZEUS.STG.CREO_FOLDERMESSAGE_HIST
WHERE ASOFDATE = TO_DATE('2023-07-03');
-- >> 15,674

SELECT 
    count(*)
    -- fm.MESSAGE_KEY
    -- ,fm.FOLDER_KEY
    -- ,fm.LOCKED_BY
    -- ,fm.IS_READ
    -- ,m.DATE_ENTERED
FROM ZEUS.STG.CREO_FOLDERMESSAGE_HIST fm
JOIN ZEUS.STG.CREO_FOLDER_HIST f ON fm.FOLDER_KEY = f.FOLDER_KEY
ORDER BY f.FOLDER_KEY DESC;
-- JOIN ZEUS.STG.CREO_MESSAGE_HIST m ON fm.MESSAGE_KEY = m.MESSAGE_KEY
-- WHERE m.DATE_ENTERED IS NULL
-- WHERE m.DATE_ENTERED <= TO_DATE('2023-07-03')
-- WHERE fm.ASOFDATE = TO_DATE('2023-07-03')
-- >> 11,289

SELECT count(*) FROM ZEUS.STG.CREO_FOLDERMESSAGE_HIST;
-- >> 15,674

-- ** TEMPLATERULE **
SELECT COUNT(*) 
FROM ZEUS.STG.CREO_TEMPLATERULE_HIST
WHERE ASOFDATE = TO_DATE('2023-07-03');
-- 599




-- * * * * * * * * * EXPLORING DATASETCELL * * * * * * * * *

-- Snowflake will automatically sort the data in the order of the columns

SELECT TOP 100
    DATASET_COLUMN_KEY
    ,DATASET_ROW_KEY
    ,DATASET_VALUE_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST;

SELECT TOP 100
    DATASET_ROW_KEY
    ,DATASET_VALUE_KEY
    ,DATASET_COLUMN_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST;

SELECT TOP 100
    DATASET_VALUE_KEY
    ,DATASET_COLUMN_KEY
    ,DATASET_ROW_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST;

-- Determining columns with highest and least cardinality
-- Low cardinality = few unique values, many duplicate values
-- High cardinality = many unique values, few duplicate values

-- HIGHEST CARDINALITY: DATASET_ROW_KEY
SELECT 
    count(*),DATASET_ROW_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_ROW_KEY
ORDER BY count(*) DESC
LIMIT 1;
-->> 238 | 48,235,725

-- MODERATE CARDINALITY: DATASET_COLUMN_KEY
SELECT 
    count(*),DATASET_COLUMN_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_COLUMN_KEY
ORDER BY count(*) DESC
LIMIT 1;
-->> 21,314,232 | 16

-- LOWEST CARDINALITY: DATASET_VALUE_KEY
SELECT 
    count(*),DATASET_VALUE_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_VALUE_KEY
ORDER BY count(*) DESC
LIMIT 1;
-->> 74,756,514 | 34

-- +++++ exploring datasetrow +++++
SELECT 
    count(*),DATASET_ROW_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_ROW_KEY
ORDER BY DATASET_ROW_KEY;

-- listing columns from highest to lowest cardinality
SELECT TOP 1000
    DATASET_ROW_KEY
    ,DATASET_COLUMN_KEY
    ,DATASET_VALUE_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST;

-- getting max key
SELECT MAX(DATASET_ROW_KEY)
FROM ZEUS.STG.CREO_DatasetCell_HIST;
-- >> 527,284,433

-- getting row count for records less than max key
SELECT COUNT(*)
FROM  ZEUS.STG.CREO_DatasetCell_HIST
WHERE DATASET_ROW_KEY < 527284433;
-- >> 836,196,947




-- * * * * * * * * * EXPLORING PACKAGETEMPLATE * * * * * * * * *

-- Snowflake will automatically sort the data in the order of the columns

SELECT TOP 100
    PACKAGE_KEY,TEMPLATE_KEY
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
ORDER BY PACKAGE_KEY DESC, TEMPLATE_KEY DESC;

SELECT TOP 100
    TEMPLATE_KEY,PACKAGE_KEY
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
ORDER BY TEMPLATE_KEY DESC, PACKAGE_KEY DESC;

-- Determining columns with highest and least cardinality
-- Low cardinality = few unique values, many duplicate values
-- High cardinality = many unique values, few duplicate values

-- HIGHEST CARDINALITY: PACKAGE_KEY
SELECT count(*),PACKAGE_KEY
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
GROUP BY PACKAGE_KEY
ORDER BY count(*) DESC
LIMIT 1;
-->> 1,397	3,215,960

-- LOWEST CARDINALITY: TEMPLATE_KEY
SELECT count(*),TEMPLATE_KEY
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
GROUP BY TEMPLATE_KEY
ORDER BY count(*) DESC
LIMIT 1;
-->> 2,006	958



-- +++++ exploring PACKAGE_KEY +++++
SELECT count(*),PACKAGE_KEY
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
GROUP BY PACKAGE_KEY
ORDER BY COUNT(*) DESC; --PACKAGE_KEY;



-- listing columns from highest to lowest cardinality
SELECT TOP 1000
    PACKAGE_KEY,TEMPLATE_KEY
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST;

-- getting max key
SELECT MAX(PACKAGE_KEY)
FROM ZEUS.STG.CREO_PACKAGETEMPLATE_HIST;
-- >> 

-- getting row count for records less than max key
SELECT COUNT(*)
FROM  ZEUS.STG.CREO_PACKAGETEMPLATE_HIST
WHERE PACKAGE_KEY < 0;
-- >> 




-- * * * * * * * * * * 8/21/23 * * * * * * * * * *
--         Meeting with James and Kapil

SELECT TOP 100 
    concat(dataset_column_key, dataset_row_key, dataset_value_key)::bigint AS concat
    ,DATASET_COLUMN_KEY, DATASET_ROW_KEY, DATASET_VALUE_KEY
FROM ZEUS.STG.CREO_DatasetCell_HIST
order by 1 DESC;


-- DATASET_ROW_KEY
SELECT TOP 100 
    DATASET_ROW_KEY,
    MIN(DATASET_COLUMN_KEY), MAX(DATASET_COLUMN_KEY),
    MIN(DATASET_VALUE_KEY), MAX(DATASET_VALUE_KEY),
    COUNT(*)
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_ROW_KEY
order by COUNT(*) DESC;


-- DATASET_COLUMN_KEY
SELECT TOP 100 
    DATASET_COLUMN_KEY,
    MIN(DATASET_ROW_KEY), MAX(DATASET_ROW_KEY),
    MIN(DATASET_VALUE_KEY), MAX(DATASET_VALUE_KEY),
    COUNT(*)
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_COLUMN_KEY
order by DATASET_COLUMN_KEY DESC;


-- DATASET_VALUE_KEY
SELECT TOP 100 
    DATASET_VALUE_KEY,
    MIN(DATASET_COLUMN_KEY), MAX(DATASET_COLUMN_KEY),
    -- MIN(DATASET_ROW_KEY), MAX(DATASET_ROW_KEY),
    COUNT(*)
FROM ZEUS.STG.CREO_DatasetCell_HIST
GROUP BY DATASET_VALUE_KEY
order by DATASET_VALUE_KEY DESC;
