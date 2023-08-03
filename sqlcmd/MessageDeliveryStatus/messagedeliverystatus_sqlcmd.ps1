$delim = '|'
$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'


# ** CREOARCHIVE **
$dbCreoArchive = 'CREOArchive'
$years = @(2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $query = "SET NOCOUNT ON ; 
    SELECT *
    FROM $dbCreoArchive.[dbo].[MessageDeliveryStatus]
    WHERE year(cast(date_entered as date)) = $year;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive\MessageDeliveryStatus\MessageDeliveryStatus_Backfill_$year.csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
}

 
# ** CREOARCHIVE2 **
$dbCreoArchive2 = 'CREOArchive2'
$years = @(2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014)
for ($i = 0; $i -lt $totalIterations; $i++) {
    $query = "SET NOCOUNT ON ; 
    SELECT *
    FROM $dbCreoArchive2.[dbo].[MessageDeliveryStatus]
    WHERE year(cast(date_entered as date)) = $year;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\$dbCreoArchive2\MessageDeliveryStatus\MessageDeliveryStatus_Backfill_$year.csv"
    
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d $dbCreoArchive2 -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
}

 