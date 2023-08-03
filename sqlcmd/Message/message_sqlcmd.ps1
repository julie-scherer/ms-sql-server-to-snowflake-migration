$delim = '^^'
$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'

# # ** CREO **
# $years = @(2023, 2022)
# foreach ($year in $years) {
$dbCreo = 'CREO'
$totalRows = 16000000 
$chunkSize = 100000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    (Get-Date).ToString()

    $offset = $i * $chunkSize

    $query = "SET NOCOUNT ON ; 
    SELECT
        MESSAGE_KEY, 
        REPLACE(REPLACE(REPLACE([SUBJECT], CHAR(237), N'i'), CHAR(146), ''), CHAR(150), '') AS [SUBJECT],
        DATE_ENTERED, DATE_SENT, 
        Replace(Replace(EXCEPTION,CHAR(10),''),CHAR(13),'') AS EXCEPTION,
        CONTAINER_KEY, COMMUNICATION_MAILING_KEY, TEMPLATE_KEY, IS_PRODUCTION, DATASET_ROW_KEY, [SERVER], MESSAGE_ID, [TYPE], NUM_EXCEPTIONS, DELIVERY_STATUS_KEY, IN_REPLY_TO_MESSAGE_KEY, DIRECTION, SEND_AFTER, SEND_AFTER_MESSAGE_KEY, IS_FINISHED, 
        Replace(Replace(HEADERS,CHAR(10),''),CHAR(13),'') AS HEADERS,
        VENDOR_ID
    FROM $dbCreo.[dbo].[Message]
    ORDER BY MESSAGE_KEY
    OFFSET $offset ROWS FETCH 
    NEXT $chunkSize ROWS ONLY;"


    $localDir = "C:\Users\JulieScherer\Desktop\BCP\$dbCreo\Message"
    ## create local directory to export csv
    if(!(test-path $localDir)) {
        mkdir $localDir
    }

    $fileName = "Message_Backfill_$($i+1).csv"
    $localPath = "$localDir\$fileName"
    
    ## cmd to export csv
    sqlcmd -S $serverName -E -C -d $dbCreo -s $delim -Q $query -o $localPath -h -1 -W -k1 
    
    ## delete zip file in local path if it exists
    $localZip = "$localPath.gz"
    if(test-path $localZip) {
        del $localZip
    }

    ## zip the csv
    pigz $localPath

    ## delete zip file in destination path if it exists
    $destPath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreo\Message\$fileName"
    $destZip = "$destPath.gz"
    if(test-path $destZip) {
        del $destZip
    }

    ##  move local zip to shared drive
    move $localZip $destZip
    
    (Get-Date).ToString() 

    # (Get-Date).ToString()
    # sqlcmd -S $serverName -E -C -d $dbCreo -s $delim -Q $query -o $filePath -h -1 -W -k1 
    # if(test-path $filePath) {
    #     pigz $filePath
    # }
    # (Get-Date).ToString()
}
 

# ** CREOARCHIVE **
$dbCreoArchive = 'CREOArchive'
# $years = @(2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014)
# for ($i = 0; $i -lt $totalIterations; $i++) {
$totalRows = 16000000 
$chunkSize = 100000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize

    $query = "SET NOCOUNT ON ; 
    SELECT
        MESSAGE_KEY, 
        REPLACE(REPLACE(REPLACE([SUBJECT], CHAR(237), N'i'), CHAR(146), ''), CHAR(150), '') AS [SUBJECT],
        DATE_ENTERED, DATE_SENT, 
        Replace(Replace(EXCEPTION,CHAR(10),''),CHAR(13),'') AS EXCEPTION,
        CONTAINER_KEY, COMMUNICATION_MAILING_KEY, TEMPLATE_KEY, IS_PRODUCTION, DATASET_ROW_KEY, [SERVER], MESSAGE_ID, [TYPE], NUM_EXCEPTIONS, DELIVERY_STATUS_KEY, IN_REPLY_TO_MESSAGE_KEY, DIRECTION, SEND_AFTER, SEND_AFTER_MESSAGE_KEY, IS_FINISHED, 
        Replace(Replace(HEADERS,CHAR(10),''),CHAR(13),'') AS HEADERS,
        VENDOR_ID
    FROM $dbCreoArchive.[dbo].[Message]
    ORDER BY MESSAGE_KEY
    OFFSET $offset ROWS FETCH 
    NEXT $chunkSize ROWS ONLY;"

    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive\Message\Message_Backfill_$year.csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
}

 
# ** CREOARCHIVE2 **
# $dbCreoArchive2 = 'CREOArchive2'
# $years = @(2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014)
# for ($i = 0; $i -lt $totalIterations; $i++) {
$dbCreoArchive = 'CREO'
$totalRows = 16000000 
$chunkSize = 100000
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize

    $query = "SET NOCOUNT ON ; 
    SELECT
        MESSAGE_KEY, 
        REPLACE(REPLACE(REPLACE([SUBJECT], CHAR(237), N'i'), CHAR(146), ''), CHAR(150), '') AS [SUBJECT],
        DATE_ENTERED, DATE_SENT, 
        Replace(Replace(EXCEPTION,CHAR(10),''),CHAR(13),'') AS EXCEPTION,
        CONTAINER_KEY, COMMUNICATION_MAILING_KEY, TEMPLATE_KEY, IS_PRODUCTION, DATASET_ROW_KEY, [SERVER], MESSAGE_ID, [TYPE], NUM_EXCEPTIONS, DELIVERY_STATUS_KEY, IN_REPLY_TO_MESSAGE_KEY, DIRECTION, SEND_AFTER, SEND_AFTER_MESSAGE_KEY, IS_FINISHED, 
        Replace(Replace(HEADERS,CHAR(10),''),CHAR(13),'') AS HEADERS,
        VENDOR_ID
    FROM $dbCreoArchive2.[dbo].[Message]
    ORDER BY MESSAGE_KEY
    OFFSET $offset ROWS FETCH 
    NEXT $chunkSize ROWS ONLY;"

    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive2\Message\Message_Backfill_$year.csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive2 -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
}

 