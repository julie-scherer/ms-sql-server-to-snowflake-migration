
-- ** DATASETCELL **
-- Tables moving to CREOArchive:
-- DatasetCell
-- DatasetRow

-- Table with DATE column:
-- Dataset
--  >> join with DatasetRow on DatasetKey

-- Steps:
-- 1. Union DatasetRow >> DATASET_ROW_KEY, DATASET_KEY
-- 2. Left join #1 with CREO.Dataset table to get date entered >> DATASET_ROW_KEY, DATE ENTERED
-- 3. Filter DATE_ENTERED column using date less than backfill date and greater than backfill date - 90 days
-- 4. Union DatasetCell >> DATASET_COLUMN_KEY, DATASET_ROW_KEY, DATASET_VALUE_KEY

WITH 
    UnionDatasetRow AS (
        SELECT
            DATASET_KEY
            ,DATE_ENTERED
        FROM CREO.[dbo].[Dataset]
        UNION ALL 
        SELECT
            DATASET_KEY
            ,DATE_ENTERED
        FROM CREOArchive.[dbo].[Dataset]
        WHERE CAST([DATE_ENTERED] AS DATE) <= CAST('2023-07-17' AS DATE) -- Filter out dates for efficiency
        AND CAST([DATE_ENTERED] AS DATE) > DATEADD(DAY, -90, '2023-07-17')
    )
    ,JoinedDatasetRowDate AS (
        SELECT
            DATASET_ROW_KEY
            ,[DATE_ENTERED]
        FROM UnionDataset AS uds
        JOIN [dbo].[DATASETROW] dr  ON dr.DATASET_KEY = ds.DATASET_KEY -- Join DatasetRow with Dataset table to get DATE_ENTERED using DATASET_KEY
    )
    ,UnionDatasetCell AS (
        SELECT
            DATASET_COLUMN_KEY
            ,DATASET_ROW_KEY
            ,DATASET_VALUE_KEY
        FROM CREO.[dbo].[DatasetCell]
        UNION ALL 
        SELECT
            DATASET_COLUMN_KEY
            ,DATASET_ROW_KEY
            ,DATASET_VALUE_KEY
        FROM CREOArchive.[dbo].[DatasetCell] -- Join CREO and CREOArchive tables since some records move to archive since backfill
    )
    ,FilteredDatasetCell AS (
        SELECT 
            udc.DATASET_VALUE_KEY
            ,udc.DATASET_ROW_KEY
            ,udc.DATASET_COLUMN_KEY
            -- ,rd.DATE_ENTERED
            -- ,ROW_NUMBER() OVER(ORDER BY rd.DATE_ENTERED ASC) AS RowNum
        FROM UnionDatasetCell udc
        LEFT JOIN JoinedDatasetRowDate rd ON rd.DATASET_ROW_KEY = udc.DATASET_ROW_KEY -- Join DatasetCell with CTE with has dates filtered
    )
SELECT 
count(*)
-- DATASET_VALUE_KEY, DATASET_ROW_KEY, DATASET_COLUMN_KEY 
-- ,RowNum
FROM FilteredDatasetCell
-- ORDER BY RowNum DESC;

-->> Record row count on or before 7/17/23
-- 1680061301

-->> Record row count on or before 7/17/23 within a 90 day timespan
-- 



-->> 844305080 WHERE CAST([DATE_ENTERED] AS DATE) <= CAST('2023-07-17' AS DATE)
-->> 843795585 COUNT ALL RECORDS




-- ** PACKAGETEMPLATE **
SELECT COUNT(*)
    -- pt.PACKAGE_KEY, 
    -- pt.TEMPLATE_KEY,
    -- t.DATE_ENTERED AS template_date,
    -- p.DATE_ENTERED AS package_date
FROM [dbo].PACKAGETEMPLATE pt
LEFT JOIN [dbo].TEMPLATE t
    ON pt.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].PACKAGE p
    ON pt.PACKAGE_KEY = p.PACKAGE_KEY
WHERE pt.PACKAGE_KEY IS NOT NULL 
AND pt.TEMPLATE_KEY IS NOT NULL;
-- >> 2243377

SELECT COUNT(*) 
FROM [dbo].PACKAGETEMPLATE;
-- >> 2243377

SELECT 
count(*)
-- pt.PACKAGE_KEY, pt.TEMPLATE_KEY
FROM [dbo].PACKAGETEMPLATE pt
LEFT JOIN [dbo].TEMPLATE t
    ON pt.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].PACKAGE p
    ON pt.PACKAGE_KEY = p.PACKAGE_KEY
WHERE CAST(p.[DATE_ENTERED] AS DATE) = CAST('2023-07-20' AS DATE)
OR CAST(t.[DATE_ENTERED] AS DATE) = CAST('2023-07-20' AS DATE);
-- >> 1411




-- *** *** *** *** *** *** *** *** *** *** *** ***
-- *** *** *** *** *** *** *** *** *** *** *** ***
-- *** *** *** *** *** *** *** *** *** *** *** ***

-- ** FOLDERCONTACT **
SELECT 
count(*)
-- fc.CONTACT_KEY, fc.FOLDER_KEY
FROM [dbo].FOLDERCONTACT fc
LEFT JOIN [dbo].CONTACT c
ON fc.CONTACT_KEY = c.CONTACT_KEY
WHERE CAST(c.[DATE_ENTERED] AS DATE) <= CAST('2023-06-29' AS DATE);
-- 122 :-)

SELECT  COUNT(*) 
    -- fc.CONTACT_KEY, fc.FOLDER_KEY
FROM [dbo].FOLDERCONTACT fc
LEFT JOIN [dbo].CONTACT c
ON fc.CONTACT_KEY = c.CONTACT_KEY
WHERE fc.CONTACT_KEY IS NOT NULL 
AND fc.FOLDER_KEY IS NOT NULL;
-- 122

SELECT COUNT(*) 
FROM [dbo].FOLDERCONTACT;
-- 122

-- ** FOLDERMESSAGE **
SELECT 
    COUNT(*)
    -- top 100
    -- fm.[MESSAGE_KEY]
    -- ,fm.[FOLDER_KEY]
    -- ,fm.[LOCKED_BY]
    -- ,fm.[IS_READ]
FROM [dbo].FOLDERMESSAGE fm
JOIN [dbo].[MESSAGE] m
ON fm.MESSAGE_KEY = m.MESSAGE_KEY
JOIN [dbo].[FOLDER] f
ON f.FOLDER_KEY = fm.FOLDER_KEY
WHERE fm.MESSAGE_KEY IS NOT NULL 
AND fm.FOLDER_KEY IS NOT NULL;
-- >> 15486

SELECT COUNT(*) 
FROM [dbo].FOLDERMESSAGE;
-- >> 15486

SELECT 
    count(*)
    -- fm.[MESSAGE_KEY]
    -- ,fm.[FOLDER_KEY]
    -- ,fm.[LOCKED_BY]
    -- ,fm.[IS_READ]
FROM CREO.[dbo].FOLDERMESSAGE fm
JOIN CREO.[dbo].[FOLDER] f ON fm.FOLDER_KEY = f.FOLDER_KEY
ORDER BY f.FOLDER_KEY DESC;

SELECT COUNT(*) FROM [dbo].[MESSAGE];

-- ** TEMPLATERULE **
SELECT 
count(*)
-- tr.RULE_KEY, tr.TEMPLATE_KEY
FROM [dbo].TEMPLATERULE tr
LEFT JOIN [dbo].TEMPLATE t
    ON tr.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].[RULE] r
    ON tr.RULE_KEY = r.RULE_KEY
WHERE CAST(r.[DATE_ENTERED] AS DATE) <= CAST('2023-06-29' AS DATE)
OR CAST(t.[DATE_ENTERED] AS DATE) <= CAST('2023-06-29' AS DATE);
-- 599 :-)


SELECT COUNT(*)
FROM [dbo].TEMPLATERULE tr
LEFT JOIN [dbo].TEMPLATE t
    ON tr.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].[RULE] r
    ON tr.RULE_KEY = r.RULE_KEY
WHERE tr.RULE_KEY IS NOT NULL 
AND tr.TEMPLATE_KEY IS NOT NULL;
-- 600
