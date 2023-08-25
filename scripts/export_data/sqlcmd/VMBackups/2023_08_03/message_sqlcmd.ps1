$delim = '^^'
$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'

# # ** CREO **
$dbCreo = 'CREO'
$totalRows = 16000000 
$chunkSize = 160000
## >> 100 files
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)
for ($i = 0; $i -lt $totalIterations; $i++) {
    (Get-Date).ToString()

    $offset = $i * $chunkSize

    $query = "SET NOCOUNT ON ; 
    SELECT
        MESSAGE_KEY, 
        REPLACE(REPLACE(REPLACE(REPLACE([SUBJECT], CHAR(237), N'i'), CHAR(146), ''), CHAR(150), ''), '^', '') AS [SUBJECT],
        DATE_ENTERED, DATE_SENT, 
        Replace(Replace(EXCEPTION,CHAR(10),''),CHAR(13),'') AS EXCEPTION,
        CONTAINER_KEY, COMMUNICATION_MAILING_KEY, TEMPLATE_KEY, IS_PRODUCTION, DATASET_ROW_KEY, [SERVER], MESSAGE_ID, [TYPE], NUM_EXCEPTIONS, DELIVERY_STATUS_KEY, IN_REPLY_TO_MESSAGE_KEY, DIRECTION, SEND_AFTER, SEND_AFTER_MESSAGE_KEY, IS_FINISHED, 
        REPLACE(Replace(Replace(HEADERS,CHAR(10),''),CHAR(13),''), '^', '') AS HEADERS,
        VENDOR_ID
    FROM CREO.[dbo].[Message]
    WHERE MESSAGE_KEY = 526546123;
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