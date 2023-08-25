COPY INTO STG.SRC_WEBDIALERSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH:MM:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerStatus/WebDialerStatus_Backfill.csv';

COPY INTO STG.SRC_THIRDPARTYREFERENCETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ThirdPartyReferenceType/ThirdPartyReferenceType_Backill.csv';

COPY INTO STG.SRC_KBB_LOG_CONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/KBB_Log_Config/KBB_Log_Config_Backfill.csv';

COPY INTO STG.SRC_INTERNALPROCESSEMAILBODY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InternalProcessEmailBody/InternalProcessEmailBody_Backfill.csv';

COPY INTO STG.SRC_BOOLEANQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BooleanQuestion/BooleanQuestion_Backfill.csv';

COPY INTO STG.SRC_ABLFACILITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ABLFacility/ABLFacility_Backfill.csv';

COPY INTO STG.SRC_VARIABLERATETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VariableRateType/VariableRateType_Backfill.csv';

COPY INTO STG.SRC_TRANSACTIONDIRECTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionDirection/TransactionDirection_Backfill.csv';

COPY INTO STG.SRC_THIRDPARTY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ThirdParty/ThirdParty_Backfill.csv';

COPY INTO STG.SRC_RAWDATATYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RawDataType/RawDataType_Backfill.csv';

COPY INTO STG.SRC_PRESENTMENTPAYMENTMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentPaymentMethod/PresentmentPaymentMethod_Backfill.csv';

COPY INTO STG.SRC_PREQUALIFICATIONCONSENTSOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PreQualificationConsentSource/PreQualificationConsentSource_Backfill.csv';

COPY INTO STG.SRC_POWEROFATTORNEYTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PowerOfAttorneyType/PowerOfAttorneyType_Backfill.csv';

COPY INTO STG.SRC_PENDINGREASONRESOLVEREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PendingReasonResolveReason/PendingReasonResolveReason_Backfill.csv';

COPY INTO STG.SRC_PAYMENTMETHODPROVISIONALAPPROVALTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentMethodProvisionalApprovalType/PaymentMethodProvisionalApprovalType_Backfill.csv';

COPY INTO STG.SRC_OUTOFWALLETVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletVendor/OutOfWalletVendor_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSEMAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM') end
, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusEmail/OptPlusEmail_Backfill.csv';

COPY INTO STG.SRC_INCOMECALCULATIONCYCLE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeCalculationCycle/IncomeCalculationCycle_Backfill.csv';

COPY INTO STG.SRC_CARDFUNDINGVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardFundingVendor/CardFundingVendor_Backfill.csv';

COPY INTO STG.SRC_BUMPUPTIERTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BumpUpTierType/BumpUpTierType_Backfill.csv';

COPY INTO STG.SRC_BOOLEANQUESTIONPROCESSTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BooleanQuestionProcessType/BooleanQuestionProcessType_Backfill.csv';

COPY INTO STG.SRC_BOOLEANQUESTIONCOMPANY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BooleanQuestionCompany/BooleanQuestionCompany_Backfill.csv';

COPY INTO STG.SRC_BANKRUPTCYTRUSTEECLAIMTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankruptcyTrusteeClaimType/BankruptcyTrusteeClaimType_Backfill.csv';

COPY INTO STG.SRC_ATTORNEYTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AttorneyType/AttorneyType_Backfill.csv';

COPY INTO STG.SRC_TELLERTITLEEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerTitleEdit/TellerTitleEdit_Backfill.csv';

COPY INTO STG.SRC_INTERNALPROCESSEMAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InternalProcessEmail/InternalProcessEmail_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGNATURALDISASTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingNaturalDisaster/CreditReportingNaturalDisaster_Backfill.csv';

COPY INTO STG.SRC_COLLECTIONAGINGCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionAgingConfig/CollectionAgingConfig_Backfill.csv';

COPY INTO STG.SRC_WEBCALLQUEUESTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallQueueStatus/WebCallQueueStatus_Backfill.csv';

COPY INTO STG.SRC_TELLERTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerType/TellerType_Backfill.csv';

COPY INTO STG.SRC_TELLERTITLE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerTitle/TellerTitle_Backfill.csv';

COPY INTO STG.SRC_SCHEDULEDLOANMODTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ScheduledLoanModType/ScheduledLoanModType_Backfill.csv';

COPY INTO STG.SRC_SCHEDULEDLOANMODSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ScheduledLoanModStatus/ScheduledLoanModStatus_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanType/PTPPaymentPlanType_Backfill.csv';

COPY INTO STG.SRC_PHONESKILLSGRADE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PhoneSkillsGrade/PhoneSkillsGrade_Backfill.csv';

COPY INTO STG.SRC_PAYMENTPLANTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanType/PaymentPlanType_Backfill.csv';

COPY INTO STG.SRC_PAYMENTAUTHORIZATIONDOCUMENTREQUIREMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentAuthorizationDocumentRequirement/PaymentAuthorizationDocumentRequirement_Backfill.csv';

COPY INTO STG.SRC_MONEYGRAMLOOKUPTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyGramLookupType/MoneyGramLookupType_Backfill.csv';

COPY INTO STG.SRC_LOANTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanType/LoanType_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTBLOCKTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductBlockType/LoanProductBlockType_Backfill.csv';

COPY INTO STG.SRC_LOANFUNDINGTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFundingType/LoanFundingType_Backfill.csv';

COPY INTO STG.SRC_INCOMEVERIFICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerificationType/IncomeVerificationType_Backfill.csv';

COPY INTO STG.SRC_EXPENSETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExpenseType/ExpenseType_Backfill.csv';

COPY INTO STG.SRC_CREDITCARDRESULTCODETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardResultCodeType/CreditCardResultCodeType_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationType/CommunicationType_Backfill.csv';

COPY INTO STG.SRC_CHECKIMAGETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CheckImageType/CheckImageType_Backfill.csv';

COPY INTO STG.SRC_CERTIFICATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, TO_BINARY(HEX_ENCODE($3), 'HEX'), $4, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when contains($6, '+') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Certificate/Certificate_Backfill.csv';

COPY INTO STG.SRC_CALLCAMPAIGNQUEUESTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignQueueStatus/CallCampaignQueueStatus_Backfill.csv';

COPY INTO STG.SRC_BILLPAYBILLERSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BillPayBillerStatus/BillPayBillerStatus_Backfill.csv';

COPY INTO STG.SRC_WEBCALLQUEUECONFIGURATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallQueueConfiguration/WebCallQueueConfiguration_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSMERCHANT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusMerchant/OptPlusMerchant_Backfill.csv';

COPY INTO STG.SRC_ACHGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHGroup/ACHGroup_Backfill.csv';

COPY INTO STG.SRC_WEBPIXELVENDORREASONPULLED_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebPixelVendorReasonPulled/WebPixelVendorReasonPulled_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLRARRTYPE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRARRType/WebCallRARRType_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRESULT1HISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end,
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARResult1History/WebCallRARResult1History_Backfill.csv';

COPY INTO STG.SRC_VISITORDEVICEAUTHENTICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDeviceAuthenticationType/VisitorDeviceAuthenticationType_Backfill.csv';

COPY INTO STG.SRC_VEHICLEODOMETERCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VehicleOdometerCode/VehicleOdometerCode_Backfill.csv';

COPY INTO STG.SRC_VEHICLECONDITION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VehicleCondition/VehicleCondition_Backfill.csv';

COPY INTO STG.SRC_SCORINGPENDINGREASONXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ScoringPendingReasonXref/ScoringPendingReasonXref_Backfill.csv';

COPY INTO STG.SRC_PRESCREENQUESTIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrescreenQuestionType/PrescreenQuestionType_Backfill.csv';

COPY INTO STG.SRC_PAYMENTPLANREQUESTSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanRequestStatus/PaymentPlanRequestStatus_Backfill.csv';

COPY INTO STG.SRC_PAYMENTMETHODDEAUTHORIZATIONREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentMethodDeauthorizationReason/PaymentMethodDeauthorizationReason_Backfill.csv';

COPY INTO STG.SRC_PASSWORDTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PasswordType/PasswordType_Backfill.csv';

COPY INTO STG.SRC_MESSAGECLASS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MessageClass/MessageClass_Backfill.csv';

COPY INTO STG.SRC_LOANRATESOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanRateSource/LoanRateSource_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTROLLOVER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7,'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductRollover/LoanProductRollover_Backfill.csv';

COPY INTO STG.SRC_LOANOVERRIDETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanOverrideType/LoanOverrideType_Backfill.csv';

COPY INTO STG.SRC_LOANORIGINATIONCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanOriginationCode/LoanOriginationCode_Backfill.csv';

COPY INTO STG.SRC_LANGUAGETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LanguageType/LanguageType_Backfill.csv';

COPY INTO STG.SRC_INTERNALPROCESSEMAILTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InternalProcessEmailType/InternalProcessEmailType_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERBATCHSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterBatchStatus/FormLetterBatchStatus_Backfill.csv';

COPY INTO STG.SRC_EXPORTPROFILE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExportProfile/ExportProfile_Backfill.csv';

COPY INTO STG.SRC_ESIGNDOCMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ESignDocMethod/ESignDocMethod_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGTRANSCODEACTIVITYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingTransCodeActivityXRef/CreditReportingTransCodeActivityXRef_Backfill.csv';

COPY INTO STG.SRC_CLRDATATYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ClrDataType/ClrDataType_Backfill.csv';

COPY INTO STG.SRC_BILLPAYVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BillPayVendor/BillPayVendor_Backfill.csv';

COPY INTO STG.SRC_ADDRESSFORMAT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressFormat/AddressFormat_Backfill.csv';

COPY INTO STG.SRC_ACHLOANPAYMENTREFUNDSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHLoanPaymentRefundStatus/ACHLoanPaymentRefundStatus_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANPAYMENTSCHEDULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanPaymentSchedule/PTPPaymentPlanPaymentSchedule_Backfill.csv';

COPY INTO STG.SRC_PROMISETOPAYTIMESLOTCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PromiseToPayTimeSlotConfig/PromiseToPayTimeSlotConfig_Backfill.csv';

COPY INTO STG.SRC_MARKETINGINVITATIONOVERRIDETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MarketingInvitationOverrideType/MarketingInvitationOverrideType_Backfill.csv';

COPY INTO STG.SRC_LOANOVERRIDEREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanOverrideReason/LoanOverrideReason_Backfill.csv';

COPY INTO STG.SRC_INCOMEAMOUNTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeAmountType/IncomeAmountType_Backfill.csv';

COPY INTO STG.SRC_EMPLOYMENTSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EmploymentStatus/EmploymentStatus_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingStatus/CreditReportingStatus_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGRULETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingRuleType/CreditReportingRuleType_Backfill.csv';

COPY INTO STG.SRC_CC_STATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CC_Status/CC_Status_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCAMPAIGN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCampaign/WebCallCampaign_Backfill.csv';

COPY INTO STG.SRC_VISITORDEVICETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDeviceType/VisitorDeviceType_Backfill.csv';

COPY INTO STG.SRC_VEHICLELEGALSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VehicleLegalStatus/VehicleLegalStatus_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANCONFIGDOCUMENTXREFHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanConfigDocumentXRefHistory/PTPPaymentPlanConfigDocumentXRefHistory_Backfill.csv';

COPY INTO STG.SRC_LOANPAYMENTSTAGEDSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentStagedStatus/LoanPaymentStagedStatus_Backfill.csv';

COPY INTO STG.SRC_LOANPAYMENTSTAGEDNOTSENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentStagedNotSentReason/LoanPaymentStagedNotSentReason_Backfill.csv';

COPY INTO STG.SRC_LOANDEPOSITORDERRESETREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDepositOrderResetReason/LoanDepositOrderResetReason_Backfill.csv';

COPY INTO STG.SRC_INCOMEVERIFICATIONSOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerificationSource/IncomeVerificationSource_Backfill.csv';

COPY INTO STG.SRC_BANKSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankStatus/BankStatus_Backfill.csv';

COPY INTO STG.SRC_APIAPPLICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ApiApplication/ApiApplication_Backfill.csv';

COPY INTO STG.SRC_ADDRESSSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressStatus/AddressStatus_Backfill.csv';

-- COPY INTO STG.SRC_NOBLECONFIGURATION_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
-- $1, $2, $3, $4, $5, 
-- case when $6 = '' then null else $6 end, $7, $8, $9, $10, $11
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/NobleConfiguration/NobleConfiguration_Backfill.csv';

-- COPY INTO STG.SRC_LENDER_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
-- case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, $10, $11
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/Lender/Lender_Backfill.csv';

COPY INTO STG.SRC_WEBCALLLOGGINGCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallLoggingCategory/WebCallLoggingCategory_Backfill.csv';

COPY INTO STG.SRC_THIRDPARTYEMPLOYMENTSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ThirdPartyEmploymentStatus/ThirdPartyEmploymentStatus_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANDUEDATECHANGETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanDueDateChangeType/PTPPaymentPlanDueDateChangeType_Backfill.csv';

COPY INTO STG.SRC_PRESENTMENTREQUESTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentRequestReason/PresentmentRequestReason_Backfill.csv';

COPY INTO STG.SRC_PAYMENTSPASTDUETRANSACTIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentsPastDueTransactionType/PaymentsPastDueTransactionType_Backfill.csv';

COPY INTO STG.SRC_MARITALSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MaritalStatus/MaritalStatus_Backfill.csv';

COPY INTO STG.SRC_EMPLOYMENTREGIONS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EmploymentRegions/EmploymentRegions_Backfill.csv';

COPY INTO STG.SRC_COMPANYCREDENTIALTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyCredentialType/CompanyCredentialType_Backfill.csv';

COPY INTO STG.SRC_COLLECTIONSETTLEMENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionSettlementReason/CollectionSettlementReason_Backfill.csv';

COPY INTO STG.SRC_BUSINESSENTITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BusinessEntity/BusinessEntity_Backfill.csv';

COPY INTO STG.SRC_BANKPARENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankParent/BankParent_Backfill.csv';

COPY INTO STG.SRC_BANKCLASSIFICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankClassificationType/BankClassificationType_Backfill.csv';

COPY INTO STG.SRC_AMLTHRESHOLDTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLThresholdType/AMLThresholdType_Backfill.csv';

COPY INTO STG.SRC_ACH_RETURNCODEHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = '' then null else $11 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACH_ReturnCodeHistory/ACH_ReturnCodeHistory_Backfill.csv';

COPY INTO STG.SRC_RULEDEFTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RuleDefType/RuleDefType_Backfill.csv';

COPY INTO STG.SRC_LOANADJUSTMENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanAdjustmentType/LoanAdjustmentType_Backfill.csv';

COPY INTO STG.SRC_FUELTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FuelType/FuelType_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, 
case when $2 = 'True' then 1 else 0 end, $3, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterConfig/FormLetterConfig_Backfill.csv';

COPY INTO STG.SRC_COMPANYBANKACCOUNT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyBankAccount/CompanyBankAccount_Backfill.csv';

COPY INTO STG.SRC_CARDFUNDINGREQUESTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardFundingRequestType/CardFundingRequestType_Backfill.csv';

COPY INTO STG.SRC_ADJUSTMENTAMOUNTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AdjustmentAmountType/AdjustmentAmountType_Backfill.csv';

COPY INTO STG.SRC_VISITORAUTHENTICATIONCODECONFIGCHANNEL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorAuthenticationCodeConfigChannel/VisitorAuthenticationCodeConfigChannel_Backfill.csv';

COPY INTO STG.SRC_RISREPTDONOTCONTACTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RisReptDoNotContactType/RisReptDoNotContactType_Backfill.csv';

COPY INTO STG.SRC_NOTIFICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/NotificationType/NotificationType_Backfill.csv';

COPY INTO STG.SRC_MIMETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MimeType/MimeType_Backfill.csv';

COPY INTO STG.SRC_INSURANCECLAIMREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceClaimReason/InsuranceClaimReason_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPEAML_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeAML/IdentificationTypeAML_Backfill.csv';

COPY INTO STG.SRC_DOCUMENTSIGNATUREACTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocumentSignatureAction/DocumentSignatureAction_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFLASHQUESTIONS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFlashQuestions/CustomerFlashQuestions_Backfill.csv';

COPY INTO STG.SRC_APPLYDUETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ApplyDueType/ApplyDueType_Backfill.csv';

COPY INTO STG.SRC_AMLOCCUPATIONREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLOccupationReason/AMLOccupationReason_Backfill.csv';

COPY INTO STG.SRC_ACHPROCESSINGTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHProcessingType/ACHProcessingType_Backfill.csv';

COPY INTO STG.SRC_WEBCALLEMAILTEMPLATEATTACHMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallEmailTemplateAttachment/WebCallEmailTemplateAttachment_Backfill.csv';

COPY INTO STG.SRC_VISITORDOCUMENTTEMPLATETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDocumentTemplateType/VisitorDocumentTemplateType_Backfill.csv';

COPY INTO STG.SRC_VEHICLETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VehicleType/VehicleType_Backfill.csv';

COPY INTO STG.SRC_LOANFUNDINGSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFundingStatus/LoanFundingStatus_Backfill.csv';

COPY INTO STG.SRC_PREPAIDCARDSTOPPAYMENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, 
case when $2 = 'True' then 1 else 0 end, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrepaidCardStopPaymentReason/PrepaidCardStopPaymentReason_Backfill.csv';

COPY INTO STG.SRC_INTERESTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InterestType/InterestType_Backfill.csv';

COPY INTO STG.SRC_INSURANCECANCELREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceCancelReason/InsuranceCancelReason_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPECATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeCategory/IdentificationTypeCategory_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethod/FundingMethod_Backfill.csv';

COPY INTO STG.SRC_CONFIGURABLEQUESTIONCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ConfigurableQuestionCategory/ConfigurableQuestionCategory_Backfill.csv';

COPY INTO STG.SRC_CHANNEL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Channel/Channel_Backfill.csv';

COPY INTO STG.SRC_BUSINESSLEGALTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BusinessLegalType/BusinessLegalType_Backfill.csv';

COPY INTO STG.SRC_BUMPUPREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BumpUpReason/BumpUpReason_Backfill.csv';

COPY INTO STG.SRC_COLLECTIONAGENCY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
case when $11 = 'True' then 1 else 0 end, $12, $13, $14, $15, $16, 
case when $17 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionAgency/CollectionAgency_Backfill.csv';

COPY INTO STG.SRC_WEBCALLFEATURESHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when contains($6, '+') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallFeaturesHistory/WebCallFeaturesHistory_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCSRREPORTCOLUMN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCSRReportColumn/WebCallCSRReportColumn_Backfill.csv';

-- COPY INTO STG.SRC_SKIPTRACECONFIG_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
-- $1, 
-- case when $2 = 'True' then 1 else 0 end, $3, $4, $5, $6, $7, $8, $9
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/SkipTraceConfig/SkipTraceConfig_Backfill.csv';

COPY INTO STG.SRC_PREPAIDCARDGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrepaidCardGroup/PrepaidCardGroup_Backfill.csv';

COPY INTO STG.SRC_PHOTOIDTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PhotoIdType/PhotoIdType_Backfill.csv';

COPY INTO STG.SRC_LOANIMPORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanImport/LoanImport_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPEVERIFY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeVerify/IdentificationTypeVerify_Backfill.csv';

COPY INTO STG.SRC_DOCIMAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocImage/DocImage_Backfill.csv';

COPY INTO STG.SRC_CUROHELP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CuroHelp/CuroHelp_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGRUNSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingRunStatus/CreditReportingRunStatus_Backfill.csv';

COPY INTO STG.SRC_CAPSRUNSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CapsRunStatus/CapsRunStatus_Backfill.csv';

COPY INTO STG.SRC_ADDRESSREMOVEDREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-24'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressRemovedReason/AddressRemovedReason_Backfill.csv';

COPY INTO STG.SRC_ADDRESSREMOVEDREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressRemovedReason/AddressRemovedReason_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPEHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = 'True' then 1 else 0 end, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, $15, $16, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = '' then null else to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeHistory/IdentificationTypeHistory_Backfill.csv';

COPY INTO STG.SRC_WEBCALLQUEUETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallQueueType/WebCallQueueType_Backfill.csv';

COPY INTO STG.SRC_VISITORBLOCKTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorBlockType/VisitorBlockType_Backfill.csv';

COPY INTO STG.SRC_PHONESKILLSGRADER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PhoneSkillsGrader/PhoneSkillsGrader_Backfill.csv';

COPY INTO STG.SRC_HOLIDAY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Holiday/Holiday_Backfill.csv';

COPY INTO STG.SRC_DOCTEMPLATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocTemplate/DocTemplate_Backfill.csv';

COPY INTO STG.SRC_CLIENTAPPLICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ClientApplication/ClientApplication_Backfill.csv';

COPY INTO STG.SRC_ASPECTEXPORTJOB_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AspectExportJob/AspectExportJob_Backfill.csv';

COPY INTO STG.SRC_RISREPTDONOTCONTACTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RisReptDoNotContactReason/RisReptDoNotContactReason_Backfill.csv';

COPY INTO STG.SRC_RESUMEINTERESTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ResumeInterestType/ResumeInterestType_Backfill.csv';

COPY INTO STG.SRC_RACE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Race/Race_Backfill.csv';

COPY INTO STG.SRC_PRODUCTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProductType/ProductType_Backfill.csv';

COPY INTO STG.SRC_PRESENTMENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentType/PresentmentType_Backfill.csv';

COPY INTO STG.SRC_DEPOSITORDER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositOrder/DepositOrder_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFEEDBACKTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedbackType/CustomerFeedbackType_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFEEDBACKSOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedbackSource/CustomerFeedbackSource_Backfill.csv';

COPY INTO STG.SRC_CARDTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardType/CardType_Backfill.csv';

COPY INTO STG.SRC_CARDGOVERNORACTIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorActionType/CardGovernorActionType.csv';

COPY INTO STG.SRC_ADDRESSTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressType/AddressType_Backfill.csv';

COPY INTO STG.SRC__TMPFUNDINGMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_tmpFundingMethod/_tmpFundingMethod_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentMethod/PTPPaymentMethod_Backfill.csv';

COPY INTO STG.SRC_LOCALESETTING_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LocaleSetting/LocaleSetting_Backfill.csv';

COPY INTO STG.SRC_BUSINESSTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BusinessType/BusinessType_Backfill.csv';

COPY INTO STG.SRC_TELLERLOGINFAILREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerLoginFailReason/TellerLoginFailReason_Backfill.csv';

COPY INTO STG.SRC_REGION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = '' then null else $3 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Region/Region_Backfill.csv';

COPY INTO STG.SRC_PROCESSCONFIGINSTANCEGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessConfigInstanceGroup/ProcessConfigInstanceGroup_Backfill.csv';

COPY INTO STG.SRC_PENDINGREASONCLIENTMESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PendingReasonClientMessage/PendingReasonClientMessage_Backfill.csv';

COPY INTO STG.SRC_LOANCONFIGAPPLYPAYMENTORDER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanConfigApplyPaymentOrder/LoanConfigApplyPaymentOrder_Backfill.csv';

COPY INTO STG.SRC_INCOMEVERIFICATIONMESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = 'True' then 1 else 0 end, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerificationMessage/IncomeVerificationMessage_Backfill.csv';

COPY INTO STG.SRC_ESIGNOPTINDOC_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5,
TO_BINARY(HEX_ENCODE($6), 'HEX')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ESignOptInDoc/ESignOptInDoc_Backfill.csv';

COPY INTO STG.SRC_ESIGNLOANSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ESignLoanStatus/ESignLoanStatus_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGDISPUTESOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingDisputeSource/CreditReportingDisputeSource_Backfill.csv';

COPY INTO STG.SRC_COURTESYPAYOUTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CourtesyPayoutType/CourtesyPayoutType_Backfill.csv';

COPY INTO STG.SRC_CONFIGURABLEQUESTIONSET_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ConfigurableQuestionSet/ConfigurableQuestionSet_Backfill.csv';

COPY INTO STG.SRC__IN600631TEST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_IN600631Test/_IN600631Test_Backfill.csv';

COPY INTO STG.SRC_PENDINGREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PendingReason/PendingReason_Backfill.csv';

COPY INTO STG.SRC_LOANSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanStatus/LoanStatus_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERLEADINSERTLOANBYPHONEDENIED_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadInsertLoanByPhoneDenied/CustomerLeadInsertLoanByPhoneDenied_Backfill.csv';

COPY INTO STG.SRC_FORMLETTEREMAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = 'True' then 1 else 0 end, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterEmail/FormLetterEmail_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGDISPUTERESPONSECODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = 'True' then 1 else 0 end, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingDisputeResponseCode/CreditReportingDisputeResponseCode_Backfill.csv';

COPY INTO STG.SRC_CARDRESPONSETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardResponseType/CardResponseType_Backfill.csv';

COPY INTO STG.SRC_WEBDAILYREPORTSTATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDailyReportStates/WebDailyReportStates_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACECONFIG_RISAUDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceConfig_RisAudit/SkipTraceConfig_RisAudit_Backfill.csv';

COPY INTO STG.SRC_PAYMENTACCOUNTEVENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentAccountEventType/PaymentAccountEventType_Backfill.csv';

COPY INTO STG.SRC_OVERSHORTCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OverShortCategory/OverShortCategory_Backfill.csv';

COPY INTO STG.SRC_INCOMEJOBTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeJobType/IncomeJobType_Backfill.csv';

COPY INTO STG.SRC_DOCUMENTSIGNINGSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, 
case when $4 = '' then null else $4 end, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocumentSigningStatus/DocumentSigningStatus_Backfill.csv';

COPY INTO STG.SRC_CREDITCARDVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardVendor/CreditCardVendor_Backfill.csv';

COPY INTO STG.SRC_DOCUWARESTATUSUPDATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareStatusUpdate/DocuwareStatusUpdate_Backfill.csv';

COPY INTO STG.SRC_DEPOSITSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositStatus/DepositStatus_Backfill.csv';

COPY INTO STG.SRC_ACHREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHReason/ACHReason_Backfill.csv';

COPY INTO STG.SRC_PAYMENTAUTHTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentAuthType/PaymentAuthType_Backfill.csv';

COPY INTO STG.SRC_PAYCYCLE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PayCycle/PayCycle_Backfill.csv';

COPY INTO STG.SRC_DEBTSALEEXPORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when contains($3, '+') = True then to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DebtSaleExport/DebtSaleExport_Backfill.csv';

COPY INTO STG.SRC__RCC2022NONADASTRADRYRUN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_RCC2022NonAdAstraDryRun/_RCC2022NonAdAstraDryRun_Backfill.csv';

COPY INTO STG.SRC__RCC2022ADASTRADRYRUN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_RCC2022AdAstraDryRun/_RCC2022AdAstraDryRun_Backfill.csv';

COPY INTO STG.SRC_ACHPROCESSINGQUEUE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, $9, $10, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = '' then null else $14 end, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, 
case when $25 = '' then null else $25 end, $26, 
case when $27 = '' then null else $27 end, 
case when $28 = '' then null else $28 end, $29, $30, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:SS AM') end, $32, 
case when $33 = 'True' then 1 else 0 end, $34
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHProcessingQueue/ACHProcessingQueue_Backfill.csv';

COPY INTO STG.SRC_PREPAIDCARDTRANSACTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrepaidCardTransAction/PrepaidCardTransAction_Backfill.csv';

COPY INTO STG.SRC_EYECOLOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EyeColor/EyeColor_Backfill.csv';

COPY INTO STG.SRC_WUPRSTAT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/wuprstat/wuprstat_Backfill.csv';

COPY INTO STG.SRC_PAYMENTMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentMethod/PaymentMethod_Backfill.csv';

COPY INTO STG.SRC_DECREASEAMOUNTOWEDREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DecreaseAmountOwedReason/DecreaseAmountOwedReason_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERLEADREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadReason/CustomerLeadReason_Backfill.csv';

COPY INTO STG.SRC_CREDITVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $15 = 'True' then 1 else 0 end, 
case when $16 = '' then null else to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $20 = 'True' then 1 else 0 end, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = '' then null else to_timestamp_ntz($22, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $23 = '' then null else to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end, 
case when $26 = '' then null else to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $27 = 'True' then 1 else 0 end, 
case when $28 = 'True' then 1 else 0 end, 
case when $29 = '' then null else to_timestamp_ntz($29, 'MM/DD/YYYY HH12:MI:SS AM') end, $30, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $32 = 'True' then 1 else 0 end, $33, $34, $35, $36
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditVendor/CreditVendor_Backfill.csv';

COPY INTO STG.SRC_ADDRESSSUITE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressSuite/AddressSuite_Backfill.csv';

COPY INTO STG.SRC_WEBCALLLOANAPPSOURCEAPP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallLoanAppSourceApp/WebCallLoanAppSourceApp_Backfill.csv';

COPY INTO STG.SRC_WEBPIXELVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, 
case when $5 = 'True' then 1 else 0 end, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, $10, $11, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebPixelVendor/WebPixelVendor_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONPREFERENCEOVERRIDE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationPreferenceOverride/CommunicationPreferenceOverride_Backfill.csv';

COPY INTO STG.SRC_PERSONTITLE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PersonTitle/PersonTitle_Backfill.csv';

-- COPY INTO STG.SRC_INCOMETYPE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
-- case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/IncomeType/IncomeType_Backfill.csv';

COPY INTO STG.SRC_HAIRCOLOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/HairColor/HairColor_Backfill.csv';

COPY INTO STG.SRC_CURRENCY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = 'True' then 1 else 0 end, $8, $9, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Currency/Currency_Backfill.csv';

COPY INTO STG.SRC_PROCESSCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessConfig/ProcessConfig_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHODGROUPCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethodGroupConfigHistory/FundingMethodGroupConfigHistory_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHODGROUPCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethodGroupConfig/FundingMethodGroupConfig_Backfill.csv';

-- COPY INTO STG.SRC_CREDITREPORTINGRULE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, $4, $5, $6, $7
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/CreditReportingRule/CreditReportingRule_Backfill.csv';

-- COPY INTO STG.SRC_CREDITREPORTINGRULE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, $4, $5, $6, $7
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/CreditReportingRule/CreditReportingRule_Backfill.csv';

COPY INTO STG.SRC_APPLYPAYMENTORDER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ApplyPaymentOrder/ApplyPaymentOrder_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCAMPAIGNCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCampaignCategory/WebCallCampaignCategory_Backfill.csv';

COPY INTO STG.SRC_VISITORSECURITYQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorSecurityQuestion/VisitorSecurityQuestion_Backfill.csv';

COPY INTO STG.SRC_MONEYGRAMTRANSMISSIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyGramTransmissionType/MoneyGramTransmissionType_Backfill.csv';

COPY INTO STG.SRC_INVALIDSTREET_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InvalidStreet/InvalidStreet_Backfill.csv';

COPY INTO STG.SRC_DOCUWARECABINETTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareCabinetType/DocuwareCabinetType_Backfill.csv';

COPY INTO STG.SRC_CHANNELPAYMENTCOMMUNICATIONLIMIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ChannelPaymentCommunicationLimit/ChannelPaymentCommunicationLimit_Backfill.csv';

COPY INTO STG.SRC_WEBREFERRALMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebReferralMethod/WebReferralMethod_Backfill.csv';

COPY INTO STG.SRC_WEBPIXELVENDORDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = 'True' then 1 else 0 end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebPixelVendorDetail/WebPixelVendorDetail_Backfill.csv';

COPY INTO STG.SRC_SECURITYQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SecurityQuestion/SecurityQuestion_Backfill.csv';

COPY INTO STG.SRC_GLACCTGLOBALHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, 
case when $32 = '' then null else $32 end, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, 
case when $47 = '' then null else to_timestamp_ntz($47, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $48 = '' then null else to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctGlobalHistory/GLAcctGlobalHistory_Backfill.csv';

-- COPY INTO STG.SRC_GLACCTGLOBAL_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, 
-- case when $32 = '' then null else $32 end, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, 
-- case when $47 = '' then null else to_timestamp_ntz($47, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when $48 = '' then null else to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:SS AM') end
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/GLAcctGlobal/GLAcctGlobal_Backfill.csv';

COPY INTO STG.SRC_FEATURE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Feature/Feature_Backfill.csv';

COPY INTO STG.SRC__CH039338_PENDINGREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CH039338_PendingReason/_CH039338_PendingReason_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRFEATURESHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when contains($7, '+') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARRFeaturesHistory/WebCallRARRFeaturesHistory_Backfill.csv';

COPY INTO STG.SRC_NOTETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/NoteType/NoteType_Backfill.csv';

COPY INTO STG.SRC_PROCESSCONFIGINSTANCETELLER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessConfigInstanceTeller/ProcessConfigInstanceTeller_Backfill.csv';

COPY INTO STG.SRC_EMAILDISPOSITION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EmailDisposition/EmailDisposition_Backfill.csv';

COPY INTO STG.SRC_CHECKPAYMENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CheckPaymentType/CheckPaymentType_Backfill.csv';

COPY INTO STG.SRC_ASPECTDIMENSIONID_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AspectDimensionId/AspectDimensionId_Backfill.csv';

COPY INTO STG.SRC_WEBCALLQUICKNOTE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else $7 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallQuickNote/WebCallQuickNote_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFEEDBACKRESOLUTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedbackResolution/CustomerFeedbackResolution_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANSECURITYGROUPHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanSecurityGroupHistory/PTPPaymentPlanSecurityGroupHistory_Backfill.csv';

COPY INTO STG.SRC_LOANAPPLICATIONTHIRDPARTYINCOME_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, $5, $6, $7, $8, 
case when $9 = '' then null else $9 end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationThirdPartyIncome/LoanApplicationThirdPartyIncome_Backfill.csv';

COPY INTO STG.SRC_CALLCAMPAIGNQUEUESTATUSREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignQueueStatusReason/CallCampaignQueueStatusReason_Backfill.csv';

COPY INTO STG.SRC_AMLTHRESHOLDRULETRANSXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLThresholdRuleTransXref/AMLThresholdRuleTransXref_Backfill.csv';

COPY INTO STG.SRC_WEBCALLWEBEMERGENCYALERTTEMPLATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallWebEmergencyAlertTemplate/WebCallWebEmergencyAlertTemplate_Backfill.csv';

ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'MM/DD/YYYY HH12:MI:SS AM';

-- COPY INTO STG.SRC_WEBCALLRARRGROUP_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, 
-- case when $3 = 'True' then 1 else 0 end, $4, $5, 
-- case when contains($6, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') 
-- when $6 = '0' then try_to_timestamp($6, 'MM/DD/YYYY HH12:MI:SS AM')
-- else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $7, $8, $9
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRarrGroup/WebCallRarrGroup_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERLEADSTATUSREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadStatusReason/CustomerLeadStatusReason_Backfill.csv';

COPY INTO STG.SRC_CAPSSKIPREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CapsSkipReason/CapsSkipReason_Backfill.csv';

COPY INTO STG.SRC_PENDINGREASONCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PendingReasonConfig/PendingReasonConfig_Backfill.csv';

COPY INTO STG.SRC_INTERPERSONALRELATIONSHIPTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InterpersonalRelationshipType/InterpersonalRelationshipType_Backfill.csv';

COPY INTO STG.SRC__ACHBANKFOR2104POSTSCRIPT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, $5, 
case when $6 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_ACHBankFor2104PostScript/_ACHBankFor2104PostScript_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTFINANCIALGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductFinancialGroup/LoanProductFinancialGroup_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSGLOBAL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = 'True' then 1 else 0 end, $16, $17, $18, $19, $20, $21, $22, $23
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusGlobal/OptPlusGlobal_Backfill.csv';

COPY INTO STG.SRC_COMPANYEXPENSETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyExpenseType/CompanyExpenseType_Backfill.csv';

COPY INTO STG.SRC_DOCUWARECABINETEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareCabinetEdit/DocuwareCabinetEdit_Backfill.csv';

COPY INTO STG.SRC_CHK_TYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/chk_type/chk_type_Backfill.csv';

COPY INTO STG.SRC_EXPORTPROFILEPATH_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExportProfilePath/ExportProfilePath_Backfill.csv';

COPY INTO STG.SRC_BLOCKREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = '' then null else $3 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BlockReason/BlockReason_Backfill.csv';

COPY INTO STG.SRC_DISCOUNTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DiscountType/DiscountType_Backfill.csv';

COPY INTO STG.SRC_WEBLEADBUYERS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadBuyers/WebLeadBuyers_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCSRREPORTCOLUMNRARR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCSRReportColumnRARR/WebCallCSRReportColumnRARR_Backfill.csv';

-- COPY INTO STG.SRC_SKIPTRACEVENDOR_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, $4, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when $6 = 'True' then 1 else 0 end, $7, $8, $9, $10, $11, $12, $13, $14, $15
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/SkipTraceVendor/SkipTraceVendor_Backfill.csv';

COPY INTO STG.SRC_PREPAIDCARDBIN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, $9, $10, $11, 
case when $12 = '' then null else $12 end, $13, $14, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrepaidCardBin/PrepaidCardBin_Backfill.csv';

COPY INTO STG.SRC_GOLDITEM_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldItem/GoldItem_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONCONSENTCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else $7 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationConsentConfigHistory/CommunicationConsentConfigHistory_Backfill.csv';

COPY INTO STG.SRC_EMAILATTACHMENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EmailAttachmentType/EmailAttachmentType_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONCONSENTCONFIGCOMMUNICATIONEVENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationConsentConfigCommunicationEvent/CommunicationConsentConfigCommunicationEvent_Backfill.csv';

COPY INTO STG.SRC_CREDITCARDBRAND_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardBrand/CreditCardBrand_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERLEADACTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadAction/CustomerLeadAction_Backfill.csv';

COPY INTO STG.SRC_COMPANYDETAILHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyDetailHistory/CompanyDetailHistory_Backfill.csv';

COPY INTO STG.SRC_CARDGOVERNORVALIDATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, 
case when $6 = 'True' then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorValidation/CardGovernorValidation_Backfill.csv';

COPY INTO STG.SRC_AMLTHRESHOLDRULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = 'True' then 1 else 0 end, 
case when $16 = 'True' then 1 else 0 end, $17, $18, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:SS AM') end, $20, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLThresholdRule/AMLThresholdRule_Backfill.csv';

COPY INTO STG.SRC_MARKETS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Markets/Markets_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTFEATURETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductFeatureType/LoanProductFeatureType_Backfill.csv';

COPY INTO STG.SRC_ASPECTEXPORTJOBADDONMETRICIDXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AspectExportJobAddOnMetricIdXref/AspectExportJobAddOnMetricIdXref_Backfill.csv';

COPY INTO STG.SRC_GALILEOALERTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GalileoAlertType/GalileoAlertType_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERLOANDOCUMENTSSTATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterLoanDocumentsState/FormLetterLoanDocumentsState_Backfill.csv';

COPY INTO STG.SRC_REASONFORARREARS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ReasonForArrears/ReasonForArrears_Backfill.csv';

-- COPY INTO STG.SRC_GLOBAL_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92, $93, $94, $95, $96, $97, $98, $99
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/Global/Global_Backfill.csv';

-- COPY INTO STG.SRC_VISITORDOCUMENTTEMPLATE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
-- $1, $2, $3, $4, $5, 
-- TO_BINARY(HEX_ENCODE($6), 'HEX'), $7, $8, 
-- case when $9 = 'True' then 1 else 0 end, $10, 
-- case when contains($11, '+') = True then to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $12, $13
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/VisitorDocumentTemplate/VisitorDocumentTemplate_Backfill.csv';

COPY INTO STG.SRC_INSURANCESTATUSHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceStatusHistory/InsuranceStatusHistory_Backfill.csv';

COPY INTO STG.SRC_INSURANCESTATUSHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-02'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceStatusHistory/InsuranceStatusHistory_Backfill.csv';
