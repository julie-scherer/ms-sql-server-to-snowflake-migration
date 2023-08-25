$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
$delim = '^^'

# Define the total number of rows and the chunk size (number of rows per export)
$start = 371
$totalRows = 800000000
$chunkSize = 500000
## >> 1600 files

# Calculate the number of iterations required based on the total rows and chunk size
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)

# Loop through the desired offsets to fetch data in chunks
for ($i = $start; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    
    $query = "SET NOCOUNT ON ; 
    SELECT * FROM CREO.[dbo].[DatasetValue] 
    ORDER BY DATASET_VALUE_KEY 
    OFFSET $offset ROWS 
    FETCH NEXT $chunkSize ROWS ONLY;"
    
    $fileName = "DatasetValue_Backfill_$($i+1).csv"
    $localDir = "C:\Users\JulieScherer\Desktop\BCP\CREO\DatasetValue"
    $localPath = "$localDir\$fileName"
    $destPath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DatasetValue\$fileName"
    
    (Get-Date).ToString()

    ## create local directory to export csv
    if(!(test-path $localDir)) { mkdir $localDir }
    
    ## cmd to export csv
    sqlcmd -S $serverName -E -C -d CREO -s $delim -Q $query -o $localPath -h -1 -W -k1 
    
    ## delete zip file in local path if it exists
    $localZip = "$localPath.gz"
    if(test-path $localZip) { del $localZip }

    ## zip the csv
    pigz $localPath

    ## delete zip file in destination path if it exists
    $destZip = "$destPath.gz"
    if(test-path $destZip) { del $destZip }

    ## move local zip to shared drive
    move $localZip $destZip
    
    (Get-Date).ToString()
} 
 
