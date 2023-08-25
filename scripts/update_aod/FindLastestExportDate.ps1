$sharedDrive = "\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO"

$folderPaths = @(
    "$sharedDrive\ApprovalRequest",
    "$sharedDrive\ApprovalRequestItem",
    "$sharedDrive\Campaign",
    "$sharedDrive\CampaignType",
    "$sharedDrive\Communication",
    "$sharedDrive\CommunicationMailing",
    "$sharedDrive\Config",
    "$sharedDrive\ConfigHistory",
    "$sharedDrive\ContactType",
    "$sharedDrive\Container",
    "$sharedDrive\Dataset",
    "$sharedDrive\DatasetColumn",
    "$sharedDrive\Datasource",
    "$sharedDrive\DeadMessages",
    "$sharedDrive\DeadMessages2",
    "$sharedDrive\DeliveryStatus",
    "$sharedDrive\Emoji",
    "$sharedDrive\Folder",
    "$sharedDrive\FolderContact",
    "$sharedDrive\FolderMessage",
    "$sharedDrive\Global",
    "$sharedDrive\Log",
    "$sharedDrive\MessageContact",
    "$sharedDrive\MessageContactType",
    "$sharedDrive\MessagePart",
    "$sharedDrive\MessageStatusQueue",
    "$sharedDrive\MessageType",
    "$sharedDrive\Package",
    "$sharedDrive\Parameter",
    "$sharedDrive\Rule",
    "$sharedDrive\Template",
    "$sharedDrive\TemplateRule",
    "$sharedDrive\TemplateType",
    "$sharedDrive\TempMessage",
    "$sharedDrive\User",
    "$sharedDrive\WebHook",
    "$sharedDrive\DatasetCell",
    "$sharedDrive\DatasetRow",
    "$sharedDrive\MessageContactV2",
    "$sharedDrive\PackageTemplate",
    "$sharedDrive\MessageDeliveryStatus",
    "$sharedDrive\MessagePartV2",
    "$sharedDrive\Message",
    "$sharedDrive\DatasetValue"
)


$result = @()
$outDir = "C:\Users\JulieScherer\Desktop\BCP"

foreach ($folderPath in $folderPaths) {
    $latestCsv = Get-ChildItem -Path $folderPath -Filter "*.csv" | Sort-Object CreationTime -Descending | Select-Object -First 1

    if ($latestCsv) {
        $dateFormatted = $latestCsv.CreationTime.ToString("yyyy-MM-dd")
        $result += [PSCustomObject]@{
            FolderPath = $folderPath
            LatestCsvCreationDate = $dateFormatted
        }
    }
}

$result | Export-Csv -Path "$outDir\File.csv" -NoTypeInformation
