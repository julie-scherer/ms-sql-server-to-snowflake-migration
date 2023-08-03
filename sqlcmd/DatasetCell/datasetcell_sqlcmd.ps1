$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
$delim = '|'

# ** CREO **
# $dbCreo = 'CREO'
# $totalRows = 815000000
# $chunkSize = 10000000
# $totalIterations = [math]::Ceiling($totalRows / $chunkSize)
# for ($i = 0; $i -lt $totalIterations; $i++) {
#     $offset = $i * $chunkSize
#     $query = "SET NOCOUNT ON ; 
# 	SELECT * FROM $dbCreo.[dbo].[DatasetCell] 
# 	ORDER BY DATASET_ROW_KEY, DATASET_COLUMN_KEY, DATASET_VALUE_KEY 
# 	OFFSET $offset ROWS 
# 	FETCH NEXT $chunkSize ROWS ONLY;"
#     $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreo\DatasetCell\DatasetCell_Backfill_$($i+1).csv"
    
#     (Get-Date).ToString()
#     sqlcmd -S $serverName -E -C -d $dbCreo -s $delim -Q $query -o $filePath -h -1 -W -k1 
#     if(test-path $filePath) {
#         pigz $filePath
#     }
#     (Get-Date).ToString()
# } 
 
# ** CREOArchive **
$dbCreoArchive = 'CREOArchive'
$totalRows = 660000000 
$chunkSize = 10000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    $query = "SET NOCOUNT ON ; 
	SELECT * FROM $dbCreoArchive.[dbo].[DatasetCell] 
	ORDER BY DATASET_ROW_KEY, DATASET_COLUMN_KEY, DATASET_VALUE_KEY 
	OFFSET $offset ROWS 
	FETCH NEXT $chunkSize ROWS ONLY;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive\DatasetCell\DatasetCell_Backfill_$($i+1).csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
} 
 
# ** CREOArchive2 **
$dbCreoArchive2 = 'CREOArchive2'
$totalRows = 5500000000 
$chunkSize = 10000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    $query = "SET NOCOUNT ON ; 
	SELECT * FROM $dbCreoArchive2.[dbo].[DatasetCell] 
	ORDER BY DATASET_ROW_KEY, DATASET_COLUMN_KEY, DATASET_VALUE_KEY 
	OFFSET $offset ROWS 
	FETCH NEXT $chunkSize ROWS ONLY;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive2\DatasetCell\DatasetCell_Backfill_$($i+1).csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive2 -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
} 
 
