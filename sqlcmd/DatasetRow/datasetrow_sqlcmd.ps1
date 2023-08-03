$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
$delim = '|'
 
# ** CREOArchive **
$dbCreoArchive = 'CREOArchive'
$totalRows = 16000000  
$chunkSize = 1000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    $query = "SET NOCOUNT ON ; 
	SELECT * FROM $dbCreoArchive.[dbo].[DatasetRow] 
	ORDER BY DATASET_ROW_KEY, DATASET_KEY
	OFFSET $offset ROWS 
	FETCH NEXT $chunkSize ROWS ONLY;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive\DatasetRow\DatasetRow_Backfill_$($i+1).csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
} 
 
# ** CREOArchive2 **
$dbCreoArchive2 = 'CREOArchive2'
$totalRows = 132000000  
$chunkSize = 1000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    $query = "SET NOCOUNT ON ; 
	SELECT * FROM $dbCreoArchive2.[dbo].[DatasetRow] 
	ORDER BY DATASET_ROW_KEY, DATASET_KEY
	OFFSET $offset ROWS 
	FETCH NEXT $chunkSize ROWS ONLY;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive2\DatasetRow\DatasetRow_Backfill_$($i+1).csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive2 -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
} 
 
