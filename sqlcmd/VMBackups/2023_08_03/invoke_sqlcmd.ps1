$delim = '|'
$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'

# Define the years for which you want to export data
$years = @(2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016)

foreach ($year in $years) {
    $query = "SET NOCOUNT ON ; SELECT * from CREO.dbo.message where year(cast(date_entered as date)) = $year;"
    $filePath = "C:\Users\JulieScherer\Desktop\BCP\CREO\Message\Message_Backfill_$year.csv"
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d CREO -s $delim -Q $query -o $filePath -h -1 -W -k1 
    (Get-Date).ToString()
}
