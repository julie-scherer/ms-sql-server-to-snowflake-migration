USE CREOArchive;
-- USE CREOArchive2;
-- SELECT COUNT(*) FROM dbo.[Message];

-- Create a temporary table to store the results
DROP TABLE IF EXISTS #TableStats;
CREATE TABLE #TableStats (
    [TableName] [VARCHAR](128),
    [RowCount] [NUMERIC],
);

-- Cursor to iterate through each table
DECLARE @TableName NVARCHAR(128);
DECLARE @RowCount NUMERIC;

DECLARE tableCursor CURSOR FOR
SELECT name
FROM sys.tables;

OPEN tableCursor;
FETCH NEXT FROM tableCursor INTO @TableName;

-- Loop through each table
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @RowCount = (
        SELECT COUNT(*)
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(@TableName)
    );
    -- Insert into #TableStats
    INSERT INTO #TableStats (TableName, [RowCount])
    VALUES (@TableName, @RowCount);

    FETCH NEXT FROM tableCursor INTO @TableName;
END;

CLOSE tableCursor;
DEALLOCATE tableCursor;

-- Retrieve the results
SELECT TableName, [RowCount]
FROM #TableStats;
