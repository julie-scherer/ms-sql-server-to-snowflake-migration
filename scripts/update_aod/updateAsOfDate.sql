USE ARES;

-- * * * * * * * * UPDATING ASOFDATE ONLY * * * * * * * *

-- // TABLE 1: ApprovalRequest
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\ApprovalRequest
UPDATE STG.CREO_ApprovalRequest_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_ApprovalRequest_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 2: ApprovalRequestItem
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\ApprovalRequestItem
UPDATE STG.CREO_ApprovalRequestItem_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_ApprovalRequestItem_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 3: Campaign
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Campaign
UPDATE STG.CREO_Campaign_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Campaign_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 4: CampaignType
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\CampaignType
UPDATE STG.CREO_CampaignType_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_CampaignType_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 5: Communication
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Communication
UPDATE STG.CREO_Communication_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Communication_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 6: CommunicationMailing
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\CommunicationMailing
UPDATE STG.CREO_CommunicationMailing_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_CommunicationMailing_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 7: Config
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Config
UPDATE STG.CREO_Config_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Config_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 8: ConfigHistory
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\ConfigHistory
UPDATE STG.CREO_ConfigHistory_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_ConfigHistory_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 9: ContactType
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\ContactType
UPDATE STG.CREO_ContactType_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_ContactType_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 10: Container
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Container
UPDATE STG.CREO_Container_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Container_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 11: Dataset
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Dataset
UPDATE STG.CREO_Dataset_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Dataset_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 12: DatasetColumn
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DatasetColumn
UPDATE STG.CREO_DatasetColumn_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_DatasetColumn_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 13: Datasource
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Datasource
UPDATE STG.CREO_Datasource_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Datasource_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 14: DeadMessages
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DeadMessages
UPDATE STG.CREO_DeadMessages_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_DeadMessages_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 15: DeadMessages2
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DeadMessages2
UPDATE STG.CREO_DeadMessages2_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_DeadMessages2_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 16: DeliveryStatus
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DeliveryStatus
UPDATE STG.CREO_DeliveryStatus_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_DeliveryStatus_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 17: Emoji
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Emoji
UPDATE STG.CREO_Emoji_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Emoji_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 18: Folder
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Folder
UPDATE STG.CREO_Folder_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Folder_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 19: FolderContact
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\FolderContact
UPDATE STG.CREO_FolderContact_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_FolderContact_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 20: FolderMessage
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\FolderMessage
UPDATE STG.CREO_FolderMessage_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_FolderMessage_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 21: Global
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Global
UPDATE STG.CREO_Global_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Global_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 22: Log
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Log
UPDATE STG.CREO_Log_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Log_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 23: MessageContact
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessageContact
UPDATE STG.CREO_MessageContact_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_MessageContact_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 24: MessageContactType
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessageContactType
UPDATE STG.CREO_MessageContactType_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_MessageContactType_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 25: MessagePart
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessagePart
UPDATE STG.CREO_MessagePart_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_MessagePart_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 26: MessageStatusQueue
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessageStatusQueue
UPDATE STG.CREO_MessageStatusQueue_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_MessageStatusQueue_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 27: MessageType
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessageType
UPDATE STG.CREO_MessageType_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_MessageType_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 28: Package
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Package
UPDATE STG.CREO_Package_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Package_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 29: Parameter
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Parameter
UPDATE STG.CREO_Parameter_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Parameter_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 30: Rule
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Rule
UPDATE STG.CREO_Rule_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Rule_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 31: Template
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Template
UPDATE STG.CREO_Template_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_Template_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 32: TemplateRule
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\TemplateRule
UPDATE STG.CREO_TemplateRule_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_TemplateRule_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 33: TemplateType
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\TemplateType
UPDATE STG.CREO_TemplateType_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_TemplateType_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 34: TempMessage
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\TempMessage
UPDATE STG.CREO_TempMessage_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_TempMessage_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 35: User
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\User
UPDATE STG.CREO_User_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_User_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 36: WebHook
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\WebHook
UPDATE STG.CREO_WebHook_HIST
    SET ASOFDATE = TO_DATE('2023-06-27')
WHERE ASOFDATE = to_date('2023-07-03');
-- SELECT COUNT(*) FROM STG.CREO_WebHook_HIST WHERE ASOFDATE = to_date('2023-07-03');

-- // TABLE 37: DatasetCell
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DatasetCell
UPDATE STG.CREO_DatasetCell_HIST
    SET ASOFDATE = TO_DATE('2023-07-17')
WHERE ASOFDATE = to_date('2023-07-20');
-- SELECT COUNT(*) FROM STG.CREO_DatasetCell_HIST WHERE ASOFDATE = to_date('2023-07-20');

-- // TABLE 38: DatasetRow
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DatasetRow
UPDATE STG.CREO_DatasetRow_HIST
    SET ASOFDATE = TO_DATE('2023-07-17')
WHERE ASOFDATE = to_date('2023-07-20');
-- SELECT COUNT(*) FROM STG.CREO_DatasetRow_HIST WHERE ASOFDATE = to_date('2023-07-20');

-- // TABLE 41: Contact
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Contact
UPDATE STG.CREO_Contact_HIST
    SET ASOFDATE = TO_DATE('2023-07-25')
WHERE ASOFDATE = to_date('2023-07-28');
-- SELECT COUNT(*) FROM STG.CREO_Contact_HIST WHERE ASOFDATE = to_date('2023-07-28');

-- // TABLE 42: MessageDeliveryStatus
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessageDeliveryStatus
UPDATE STG.CREO_MessageDeliveryStatus_HIST
    SET ASOFDATE = TO_DATE('2023-07-25')
WHERE ASOFDATE = to_date('2023-07-27');
-- SELECT COUNT(*) FROM STG.CREO_MessageDeliveryStatus_HIST WHERE ASOFDATE = to_date('2023-07-27');

-- // TABLE 45: DatasetValue
-- >> \\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\DatasetValue
UPDATE STG.CREO_DatasetValue_HIST
    SET ASOFDATE = TO_DATE('2023-08-13')
WHERE ASOFDATE = to_date('2023-08-03');
-- SELECT COUNT(*) FROM STG.CREO_DatasetValue_HIST WHERE ASOFDATE = to_date('2023-08-03');



-- * * * * * * * * * UPDATING ASOFDATE * * * * * * * * *
-- * * * * * * * * *  AND FILE FORMAT  * * * * * * * * *

-- /*

-- // TABLE 37: DatasetCell
COPY INTO STG.CREO_DatasetCell_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-17'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_COLUMN_KEY INT NOT NULL
		($3)::bigint 	-- $3: DATASET_VALUE_KEY BIGINT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetCell/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = '.*DatasetCell_Backfill_0.csv';

-- // TABLE 38: DatasetRow
COPY INTO STG.CREO_DatasetRow_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-17'), 
        ($1)::bigint, 	-- $1: DATASET_ROW_KEY BIGINT NOT NULL
		($2)::int, 	-- $2: DATASET_KEY INT NOT NULL
		($3)::boolean 	-- $3: HAS_MESSAGE BOOLEAN NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/DatasetRow/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = '.*DatasetRow_Backfill_0.csv';

-- // TABLE 39: MessageContactV2
COPY INTO STG.CREO_MessageContactV2_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-17'), 
        ($1)::bigint, 	-- $1: MESSAGE_CONTACT_KEY BIGINT NOT NULL
		($2)::bigint, 	-- $2: MESSAGE_KEY BIGINT NOT NULL
		($3)::int, 	-- $3: CONTACT_KEY INT NOT NULL
		($4)::int 	-- $4: TYPE INT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/MessageContactV2/
)
FILE_FORMAT = (
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = '.*MessageContactV2_Backfill_0.csv';

-- // TABLE 40: PackageTemplate
COPY INTO STG.CREO_PackageTemplate_HIST FROM (
    SELECT 
        METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-17'), 
        ($1)::int, 	-- $1: PACKAGE_KEY INT NOT NULL
		($2)::int 	-- $2: TEMPLATE_KEY INT NOT NULL
    FROM @ETL.INBOUND/CREO/Backfill/PackageTemplate/
)
FILE_FORMAT = (
    -- FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ
    FORMAT_NAME = STG.LD_CSV_PIPE_SH0_EON_GZ
)
PATTERN = '.*PackageTemplate_Backfill_0.csv';

-- */