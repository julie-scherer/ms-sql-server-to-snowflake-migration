USE CREO;
-- USE CREOArchive;
-- USE CREOArchive2;

-- Declare a table variable to store the results
DROP TABLE IF EXISTS #TableStats;
DECLARE @TableStats TABLE (
    TableName VARCHAR(128),
    [RowCount] BIGINT
);

-- Insert table names and row counts into @TableStats
INSERT INTO @TableStats (TableName, [RowCount])
SELECT QUOTENAME(t.name) AS TableName, p.rows AS [RowCount]
FROM sys.tables t
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1); -- Consider only heap and clustered index rows

-- Retrieve the results
SELECT TableName, [RowCount]
FROM @TableStats
ORDER BY TableName;
