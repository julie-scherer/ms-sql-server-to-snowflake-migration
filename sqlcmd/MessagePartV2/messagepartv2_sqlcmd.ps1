$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
$delim = '|'

$dbCreo = 'CREO'
$totalRows = 16000000 
$chunkSize = 1000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
# Loop through the desired offsets to fetch data in chunks
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    
    $query = "SET NOCOUNT ON ; 
    SELECT * FROM $dbCreo.[dbo].[MessagePartV2] 
    ORDER BY MESSAGE_PART_KEY 
    OFFSET $offset ROWS 
    FETCH NEXT $chunkSize ROWS ONLY;"

    # $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreo\MessagePartV2\MessagePartV2_Backfill_$($i+1).csv"
    $fileName = "MessagePartV2_Backfill_$($i+1).csv"
    $localDir = "C:\Users\JulieScherer\Desktop\BCP\$dbCreo\MessagePartV2"
    $localPath = "$localDir\$fileName"
    $destPath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreo\MessagePartV2\$fileName"
    
    (Get-Date).ToString()

    ## create local directory to export csv
    if(!(test-path $localDir)) { mkdir $localDir }
    
    ## cmd to export csv
    sqlcmd -S $serverName -E -C -d $dbCreo -s $delim -Q $query -o $localPath -h -1 -W -k1 
    
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



$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
$delim = '|'


# ** CREOARCHIVE **
$dbCreoArchive = 'CREOArchive'
$totalRows = 16000000
$chunkSize = 1000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize

    $query = "SET NOCOUNT ON ;
    SELECT * FROM $dbCreoArchive.[dbo].[MessagePartV2]
    ORDER BY MESSAGE_PART_KEY, MESSAGE_KEY
    OFFSET $offset ROWS FETCH
    NEXT $chunkSize ROWS ONLY;"

    $fileName = "MessagePartV2_Backfill_$($i+1).csv"
    $localDir = "C:\Users\JulieScherer\Desktop\BCP\$dbCreoArchive\MessagePartV2"
    $localPath = "$localDir\$fileName"
    $destPath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive\MessagePartV2\$fileName"
    
    (Get-Date).ToString()

    ## create local directory to export csv
    if(!(test-path $localDir)) { mkdir $localDir }
    
    ## cmd to export csv
    sqlcmd -S $serverName -E -C -d $dbCreoArchive -s $delim -Q $query -o $localPath -h -1 -W -k1 
    
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

 
# ** CREOARCHIVE2 **
$dbCreoArchive2 = 'CREOArchive2'
$totalRows = 140000000 #132,901,023 
$chunkSize = 1000000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize

    $query = "SET NOCOUNT ON ; 
    SELECT * FROM $dbCreoArchive.[dbo].[MessagePartV2] 
    ORDER BY MESSAGE_PART_KEY, MESSAGE_KEY 
    OFFSET $offset ROWS FETCH 
    NEXT $chunkSize ROWS ONLY;"

    $fileName = "MessagePartV2_Backfill_$($i+1).csv"
    $localDir = "C:\Users\JulieScherer\Desktop\BCP\$dbCreoArchive2\MessagePartV2"
    $localPath = "$localDir\$fileName"
    $destPath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive2\MessagePartV2\$fileName"
    
    (Get-Date).ToString()

    ## create local directory to export csv
    if(!(test-path $localDir)) { mkdir $localDir }
    
    ## cmd to export csv
    sqlcmd -S $serverName -E -C -d $dbCreoArchive2 -s $delim -Q $query -o $localPath -h -1 -W -k1 
    
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

 