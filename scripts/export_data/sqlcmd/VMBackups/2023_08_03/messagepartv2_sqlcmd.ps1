$serverName = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'
$delim = '|'

# Define the total number of rows and the chunk size (number of rows per export)
$totalRows = 1600000
$chunkSize = 1000000

# Calculate the number of iterations required based on the total rows and chunk size
$totalIterations = [math]::Ceiling($totalRows / $chunkSize)

# Loop through the desired offsets to fetch data in chunks
for ($i = 0; $i -lt $totalIterations; $i++) {
    $offset = $i * $chunkSize
    $query = "SET NOCOUNT ON ; SELECT * FROM CREO.[dbo].[MessagePartV2] ORDER BY MESSAGE_PART_KEY OFFSET $offset ROWS FETCH NEXT $chunkSize ROWS ONLY;"
    $filePath = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessagePartV2\MessagePartV2_Backfill_$($i+1).csv"
    (Get-Date).ToString()
    sqlcmd -S $serverName -E -C -d CREO -s $delim -Q $query -o $filePath -h -1 -W -k1 
    if(test-path $filePath) {
        pigz $filePath
    }
    (Get-Date).ToString()
}
