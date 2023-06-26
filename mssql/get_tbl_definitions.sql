USE CREO;
-- USE CREOArchive;
-- USE CREOArchive2;

-- Create a temporary table to store the results
DROP TABLE IF EXISTS #TableStats;
CREATE TABLE #TableStats (
    [TableName] [VARCHAR](128),
    [ColumnData] [VARCHAR](MAX),
);

-- Cursor to iterate through each table
DECLARE @TableName NVARCHAR(128);
DECLARE @ColumnData VARCHAR (MAX);

DECLARE tableCursor CURSOR FOR
SELECT name FROM sys.tables;

OPEN tableCursor;
FETCH NEXT FROM tableCursor INTO @TableName;

-- Loop through each table
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @ColumnData = (
        SELECT STRING_AGG(CONCAT(
            c.name, ' '
            ,(CASE
                WHEN t.name = 'varchar' THEN CONCAT('VARCHAR(', t.max_length, ')')
                WHEN t.name = 'nvarchar' THEN CONCAT('VARCHAR(', t.max_length, ')')
                WHEN t.name = 'tinyint' THEN 'SMALLINT'
                WHEN t.name = 'int' THEN 'INT'
                WHEN t.name = 'bigint' THEN 'BIGINT'
                WHEN t.name = 'decimal' THEN CONCAT('DECIMAL(', t.precision, ',', 0, ')')
                WHEN t.name = 'numeric' THEN 'NUMERIC'
                WHEN t.name = 'date' THEN 'DATE'
                WHEN t.name = 'time' THEN 'TIME'
                WHEN t.name = 'datetime2' THEN 'TIMESTAMP_LTZ'
                WHEN t.name = 'datetimeoffset' THEN 'TIMESTAMP_LTZ'
                WHEN t.name = 'real' THEN 'INT'
                WHEN t.name = 'money' THEN 'NUMBER(19,4)'
                WHEN t.name = 'smalldatetime' THEN 'TIMESTAMP_LTZ'
                WHEN t.name = 'float' THEN 'FLOAT'
                WHEN t.name = 'bit' THEN 'BOOLEAN'
                WHEN t.name = 'smallmoney' THEN 'NUMBER(10,4)'
                WHEN t.name = 'hierarchyid' THEN 'VARIANT'
                WHEN t.name = 'geometry' THEN 'VARIANT'
                WHEN t.name = 'geography' THEN 'VARIANT'
                WHEN t.name = 'varbinary' THEN 'VARIANT'
                WHEN t.name = 'binary' THEN CONCAT('BINARY(', t.max_length, ')')
                WHEN t.name = 'char' THEN CONCAT('CHAR(', t.max_length, ')')
                WHEN t.name = 'timestamp' THEN 'TIMESTAMP'
                WHEN t.name = 'sysname' THEN CONCAT('STRING(', t.max_length, ')')
                WHEN t.name = 'uniqueidentifier' THEN 'VARCHAR(36)'
                ELSE t.name
            END),' '
            ,(CASE WHEN c.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END),' '
            ,(CASE
                WHEN c.collation_name = 'SQL_Latin1_General_CP1_CI_AS' THEN 'COLLATE "en-ci"'
                ELSE ''
            END)
        ), ',')
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(@TableName)
    );

    -- Insert into #TableStats
    INSERT INTO #TableStats (TableName, ColumnData)
    VALUES (@TableName, @ColumnData);

    FETCH NEXT FROM tableCursor INTO @TableName;
END;

CLOSE tableCursor;
DEALLOCATE tableCursor;

-- Retrieve the results
SELECT *
FROM #TableStats
ORDER BY TableName;
