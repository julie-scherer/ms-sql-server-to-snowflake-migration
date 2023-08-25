COPY INTO STG.SRC_INSURANCESTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceStatus/InsuranceStatus_Backfill.csv';

COPY INTO STG.SRC_INCOMESOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeSource/IncomeSource_Backfill.csv';

COPY INTO STG.SRC_GLOBALSTATESHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, $10, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, $13, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = 'True' then 1 else 0 end, $16, $17, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = 'True' then 1 else 0 end, $20, $21, 
case when $22 = 'True' then 1 else 0 end, $23, $24, 
case when $25 = '' then null else to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $26 = '' then null else to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GlobalStatesHistory/GlobalStatesHistory_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHODGROUPITEMCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethodGroupItemConfigHistory/FundingMethodGroupItemConfigHistory_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERBATCHBUILDLETTERPROGRESS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterBatchBuildLetterProgress/FormLetterBatchBuildLetterProgress_Backfill.csv';

COPY INTO STG.SRC_REFERRALMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ReferralMethod/ReferralMethod_Backfill.csv';

COPY INTO STG.SRC_CONFIGURABLEQUESTIONNUMERICRANGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else $4 end, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ConfigurableQuestionNumericRange/ConfigurableQuestionNumericRange_Backfill.csv';

COPY INTO STG.SRC_CHECKTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CheckType/CheckType_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCHATCANNEDRESPONSES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallChatCannedResponses/WebCallChatCannedResponses_Backfill.csv';

COPY INTO STG.SRC_FCRMAMLTRANSCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FcrmAmlTransCode/FcrmAmlTransCode_Backfill.csv';

COPY INTO STG.SRC_IN466857_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IN466857/IN466857_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFEEDBACKCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedbackCategory/CustomerFeedbackCategory_Backfill.csv';

COPY INTO STG.SRC_RISTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RISTYPE/RISTYPE_Backfill.csv';

COPY INTO STG.SRC_TASKACTIONRESULTXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TaskActionResultXref/TaskActionResultXref_Backfill.csv';

COPY INTO STG.SRC_FCRMAMLTRANSCODEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FcrmAmlTransCodeXRef/FcrmAmlTransCodeXRef_Backfill.csv';

COPY INTO STG.SRC_COMPANYBANKACCOUNTGLACCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyBankAccountGLAcct/CompanyBankAccountGLAcct_Backfill.csv';

COPY INTO STG.SRC_TECODES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/tecodes/tecodes_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERLOANHISTORYSTATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterLoanHistoryState/FormLetterLoanHistoryState_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFEEDBACKTYPECATEGORYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedbackTypeCategoryXRef/CustomerFeedbackTypeCategoryXRef_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $11 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusProduct/OptPlusProduct_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTBLOCKED_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, $5, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductBlocked/LoanProductBlocked_Backfill.csv';

COPY INTO STG.SRC_CONFIGURABLEQUESTIONALLOWABLERESPONSE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ConfigurableQuestionAllowableResponse/ConfigurableQuestionAllowableResponse_Backfill.csv';

COPY INTO STG.SRC_WUCODES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/wucodes/wucodes_Backfill.csv';

COPY INTO STG.SRC_WEBCALLEMAILTEMPLATECATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallEmailTemplateCategory/WebCallEmailTemplateCategory_Backfill.csv';

COPY INTO STG.SRC_DIALERRESULTCODES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerResultCodes/DialerResultCodes_Backfill.csv';

COPY INTO STG.SRC_VISITORDOCUMENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDocumentType/VisitorDocumentType_Backfill.csv';

COPY INTO STG.SRC_VMATRANSTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VMATransType/VMATransType_Backfill.csv';

COPY INTO STG.SRC_OCRRegion_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, $6, $7, $8, $9, $10, $11, $12, $13, $14
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OCRRegion/OCRRegion_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationGroup/CommunicationGroup_Backfill.csv';

COPY INTO STG.SRC_PRESENTMENTNOTSENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentNotSentReason/PresentmentNotSentReason_Backfill.csv';

COPY INTO STG.SRC_RULEDEFSET_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RuleDefSet/RuleDefSet_Backfill.csv';

COPY INTO STG.SRC_PENDINGREASONQUESTIONSET_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PendingReasonQuestionSet/PendingReasonQuestionSet_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGDISPUTECODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingDisputeCode/CreditReportingDisputeCode_Backfill.csv';

COPY INTO STG.SRC_REFINANCELOANAPPLICATIONPENDINGREASONCONFIGURABLEQUESTIONRESPONSE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RefinanceLoanApplicationPendingReasonConfigurableQuestionResponse/RefinanceLoanApplicationPendingReasonConfigurableQuestionResponse_Backfill.csv';

COPY INTO STG.SRC_ASPECTADDONMETRICID_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AspectAddOnMetricId/AspectAddOnMetricId_Backfill.csv';

COPY INTO STG.SRC_TRANSACTIONPROCESSOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else $6 end, 
case when $7 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionProcessor/TransactionProcessor_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERLEADSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, $8, $9, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadStatus/CustomerLeadStatus_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGRARRACTIVITYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingRarrActivityXRef/CreditReportingRarrActivityXRef_Backfill.csv';

-- COPY INTO STG.SRC_FUNDINGMETHODGROUPITEMCONFIG_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, $6, $7, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/FundingMethodGroupItemConfig/FundingMethodGroupItemConfig_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTCONFIGMAINTENANCEFEERATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, 
case when $7 = '' then null when contains($7, '+') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigMaintenanceFeeRate/LoanProductConfigMaintenanceFeeRate_Backfill.csv';

COPY INTO STG.SRC_RbcEFundResponseCode_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RbcEFundResponseCode/RbcEFundResponseCode_Backfill.csv';

COPY INTO STG.SRC_ACCUMCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AccumConfig/AccumConfig_Backfill.csv';

COPY INTO STG.SRC_FORCEAPPROVALQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ForceApprovalQuestion/ForceApprovalQuestion_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSBINPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, $6, 
case when $7 = '' then null else $7 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusBinProduct/OptPlusBinProduct_Backfill.csv';

COPY INTO STG.SRC_LOANAPPLICATIONEXPENSEDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationExpenseDetail/LoanApplicationExpenseDetail_Backfill.csv';

COPY INTO STG.SRC_INCOMEVERIFYMETHODHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerifyMethodHistory/IncomeVerifyMethodHistory_Backfill.csv';

COPY INTO STG.SRC_ADDRESSSUFFIX_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AddressSuffix/AddressSuffix_Backfill.csv';

COPY INTO STG.SRC___REFACTORLOG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/__RefactorLog/__RefactorLog_Backfill.csv';

COPY INTO STG.SRC_DISTRICT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/District/District_Backfill.csv';

COPY INTO STG.SRC_EOSCARDISPUTECODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EOscarDisputeCode/EOscarDisputeCode_Backfill.csv';

COPY INTO STG.SRC_ASPECTADDONMETRICIDDIMENSIONIDXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AspectAddOnMetricIdDimensionIdXref/AspectAddOnMetricIdDimensionIdXref_Backfill.csv';

COPY INTO STG.SRC_RisTask_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RisTask/RisTask_Backfill.csv';

COPY INTO STG.SRC_GLACCTLOCATIONGROUPHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctLocationGroupHistory/GLAcctLocationGroupHistory_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONEVENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationEvent/CommunicationEvent_Backfill.csv';

-- COPY INTO STG.SRC_PTPPAYMENTPLANCONFIGDOCUMENTXREF_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/PTPPaymentPlanConfigDocumentXRef/PTPPaymentPlanConfigDocumentXRef_Backfill.csv';

-- COPY INTO STG.SRC_GLACCTLOCATIONGROUP_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/GLAcctLocationGroup/GLAcctLocationGroup_Backfill.csv';

-- COPY INTO STG.SRC_INCOMEVERIFYMETHOD_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, $8, $9
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/IncomeVerifyMethod/IncomeVerifyMethod_Backfill.csv';

COPY INTO STG.SRC_LOANAPPLICATIONEXPENSE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationExpense/LoanApplicationExpense_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERREPLACESXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterReplacesXRef/FormLetterReplacesXRef_Backfill.csv';

-- COPY INTO STG.SRC_IDENTIFICATIONTYPE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, 
-- case when $3 = '' then null else $3 end, 
-- case when $4 = 'True' then 1 else 0 end, $5, 
-- case when $6 = 'True' then 1 else 0 end, 
-- case when $7 = 'True' then 1 else 0 end, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, 
-- case when $10 = 'True' then 1 else 0 end, 
-- case when $11 = 'True' then 1 else 0 end, 
-- case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
-- case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, $15, $16, 
-- case when $17 = 'True' then 1 else 0 end, $18, $19
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/IdentificationType/IdentificationType_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONCOMMUNICATIONEVENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationCommunicationEvent/CommunicationCommunicationEvent_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANLOANPRODUCTENABLENEWLOANHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanLoanProductEnableNewLoanHistory/PTPPaymentPlanLoanProductEnableNewLoanHistory_Backfill.csv';

COPY INTO STG.SRC_WebDailyReportFields_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDailyReportFields/WebDailyReportFields_Backfill.csv';

COPY INTO STG.SRC_PRESCREENQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5, 
case when $6 = '' then null else $6 end, $7, $8, 
case when $9 = '' then null else $9 end, 
case when $10 = 'True' then 1 else 0 end, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrescreenQuestion/PrescreenQuestion_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONCONSENTCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else $7 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationConsentConfig/CommunicationConsentConfig_Backfill.csv';

COPY INTO STG.SRC__FORMLETTERONDEMAND_DELETE_201003_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_formLetterOnDemand_Delete_201003/_formLetterOnDemand_Delete_201003_Backfill.csv';

COPY INTO STG.SRC_CONFIGURABLEQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, $6, 
case when $7 = '' then null else $7 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ConfigurableQuestion/ConfigurableQuestion_Backfill.csv';

COPY INTO STG.SRC_WEBDIALERPHONELINE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerPhoneLine/WebDialerPhoneLine_Backfill.csv';

COPY INTO STG.SRC_DUALAPPROVALMESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DualApprovalMessage/DualApprovalMessage_Backfill.csv';

COPY INTO STG.SRC_RULEDEFSETDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RuleDefSetDetail/RuleDefSetDetail_Backfill.csv';

COPY INTO STG.SRC_TELLERCOMPUTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerComputer/TellerComputer_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONGROUPCHANNELVISIBILITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationGroupChannelVisibility/CommunicationGroupChannelVisibility_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTCONFIGEXPENSETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigExpenseType/LoanProductConfigExpenseType_Backfill.csv';

COPY INTO STG.SRC_CABLENDER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, 
case when $23 = '' then null else to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:SS AM') end, $24, 
case when $25 = '' then null else to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:SS AM') end, $26, 
case when $27 = 'True' then 1 else 0 end, $28, $29, $30, $31, $32
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CABLender/CABLender_Backfill.csv';

COPY INTO STG.SRC_US_STATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/US_States/US_States_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERAFTERLETTERXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterAfterLetterXRef/FormLetterAfterLetterXRef_Backfill.csv';

-- COPY INTO STG.SRC_OPTPLUSCARRIER_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, 
-- case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
-- case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, $10
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/OptPlusCarrier/OptPlusCarrier_Backfill.csv';

COPY INTO STG.SRC_WEBLEADCRITERIA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = 'True' then 1 else 0 end, $6, 
case when $7 = '' then null else $7 end, 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null else $9 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, $12, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, $15, 
case when $16 = 'True' then 1 else 0 end, $17, 
case when $18 = 'True' then 1 else 0 end, $19
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadCriteria/WebLeadCriteria_Backfill.csv';

COPY INTO STG.SRC_RIURGENTNOTE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RiUrgentNote/RiUrgentNote_Backfill.csv';

COPY INTO STG.SRC_BILLEROCRREGION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BillerOCRRegion/BillerOCRRegion_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPERULEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeRuleXRef/IdentificationTypeRuleXRef_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLFEATURES_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when contains($6, '+') then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $7, $8, $9
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallFeatures/WebCallFeatures_Backfill.csv';

COPY INTO STG.SRC_MARKETINGINVITATIONSUMMARY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MarketingInvitationSummary/MarketingInvitationSummary_Backfill.csv';

COPY INTO STG.SRC_WEBLEADCRITERIAAUDITCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadCriteriaAuditCategory/WebLeadCriteriaAuditCategory_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACESTEP_LOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else $2 end, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceStep_Location/SkipTraceStep_Location_Backfill.csv';

COPY INTO STG.SRC_RULEDEFSETDETAILEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RuleDefSetDetailEdit/RuleDefSetDetailEdit_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERMLA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') when contains($4, '-') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM')
else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null when contains($7, '+') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerMLA/CustomerMLA_Backfill.csv';

-- COPY INTO STG.SRC_VISITORAUTHENTICATIONCODECONFIG_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, 
-- case when $2 = '' then null else $2 end, 
-- case when $3 = 'True' then 1 else 0 end, 
-- case when $4 = '' then null else $4 end, 
-- case when $5 = '' then null else $5 end, 
-- case when $6 = '' then null else $6 end, 
-- case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when $8 = '' then null else $8 end, $9, $10, $11
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/VisitorAuthenticationCodeConfig/VisitorAuthenticationCodeConfig_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACESTEP_PRODUCTCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceStep_ProductCode/SkipTraceStep_ProductCode_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONGROUPCHANNEL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationGroupChannel/CommunicationGroupChannel_Backfill.csv';

COPY INTO STG.SRC_LOANADJUSTMENTBATCHSETTINGS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, 
case when contains($10, '+') = True then to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $11, $12, $13, $14, 
case when $15 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanAdjustmentBatchSettings/LoanAdjustmentBatchSettings_Backfill.csv';

COPY INTO STG.SRC_SPECIALMESSAGELOANPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SpecialMessageLoanProduct/SpecialMessageLoanProduct_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTFEATURE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductFeature/LoanProductFeature_Backfill.csv';

COPY INTO STG.SRC_TRANSCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransCode/TransCode_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLRARRACTION_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRARRAction/WebCallRARRAction_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPERULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, 
case when $4 = 'True' then 1 else 0 end, $5, $6, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeRule/IdentificationTypeRule_Backfill.csv';

COPY INTO STG.SRC_RULEDEF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RuleDef/RuleDef_Backfill.csv';

COPY INTO STG.SRC_MESSAGESCENARIO_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MessageScenario/MessageScenario_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTCONFIGMAXLOANAMTRATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigMaxLoanAmtRate/LoanProductConfigMaxLoanAmtRate_Backfill.csv';

COPY INTO STG.SRC_FCRMAMLSERVICEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FcrmAmlServiceXRef/FcrmAmlServiceXRef_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGBASESEGMENTTAG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingBaseSegmentTag/CreditReportingBaseSegmentTag_Backfill.csv';

COPY INTO STG.SRC_LOANDOCTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDocType/LoanDocType_Backfill.csv';

COPY INTO STG.SRC_EMAILTEMPLATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $3 = 'True' then 1 else 0 end, $4, $5, $6, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EmailTemplate/EmailTemplate_Backfill.csv';

-- COPY INTO STG.SRC_CALLCAMPAIGN_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, 
-- case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
-- case when contains($6, '+') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
-- case when contains($7, '+') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $8, 
-- case when $9 = 'True' then 1 else 0 end, $10, 
-- case when $11 = 'True' then 1 else 0 end, 
-- case when $12 = 'True' then 1 else 0 end, $13, $14
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/CallCampaign/CallCampaign_Backfill.csv';

COPY INTO STG.SRC_CASHEDCHECKPAYMENTREFUND_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CashedCheckPaymentRefund/CashedCheckPaymentRefund_Backfill.csv';

COPY INTO STG.SRC_PHONESKILLSSEQUENCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else $4 end, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PhoneSkillsSequence/PhoneSkillsSequence_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGACTIVITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5, 
case when $6 = 'True' then 1 else 0 end, $7, 
case when $8 = 'True' then 1 else 0 end, $9, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingActivity/CreditReportingActivity_Backfill.csv';

COPY INTO STG.SRC_PAYWARETSYSTIMEZONE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaywareTsysTimezone/PaywareTsysTimezone_Backfill.csv';

COPY INTO STG.SRC_CARDFUNDINGSTATUSCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when contains($2, '-') = True then try_to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else try_to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardFundingStatusCode/CardFundingStatusCode_Backfill.csv';

COPY INTO STG.SRC__ACHPENDINGCONVERT1905_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_achPendingConvert1905/_achPendingConvert1905_Backfill.csv';

COPY INTO STG.SRC_PRESCREENQUESTIONSTATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrescreenQuestionState/PrescreenQuestionState_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Communication/Communication_Backfill.csv';

COPY INTO STG.SRC_AREACODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AreaCode/AreaCode_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFEEDBACKSUBCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedbackSubCategory/CustomerFeedbackSubCategory_Backfill.csv';

COPY INTO STG.SRC_PROCESSCONFIGINSTANCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessConfigInstance/ProcessConfigInstance_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSEXPORTTRANSCODES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusExportTransCodes/OptPlusExportTransCodes_Backfill.csv';

COPY INTO STG.SRC_LOCALETRANSLATOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LocaleTranslator/LocaleTranslator_Backfill.csv';

COPY INTO STG.SRC_WEBDIALERRESULTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
case when $1 = '' then null else $1 end, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, 
case when $6 = 'True' then 1 else 0 end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerResultType/WebDialerResultType_Backfill.csv';

COPY INTO STG.SRC_GALILEORESPONSECODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GalileoResponseCode/GalileoResponseCode_Backfill.csv';

COPY INTO STG.SRC_CONFIGURABLEQUESTIONSETCONFIGURABLEQUESTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ConfigurableQuestionSetConfigurableQuestion/ConfigurableQuestionSetConfigurableQuestion_Backfill.csv';

COPY INTO STG.SRC_WEBLEADGEN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = 'True' then 1 else 0 end, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadGen/WebLeadGen_Backfill.csv';

COPY INTO STG.SRC_DIALERJOBCHECKSLOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerJobChecksLocation/DialerJobChecksLocation_Backfill.csv';

COPY INTO STG.SRC_WEBCALLEMAILTEMPLATETOCATEGORYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallEmailTemplateToCategoryXref/WebCallEmailTemplateToCategoryXref_Backfill.csv';

COPY INTO STG.SRC_EXTERNALAPPMASTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExternalAppMaster/ExternalAppMaster_Backfill.csv';

COPY INTO STG.SRC_TRANSUNIONCODES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransUnionCodes/TransUnionCodes_Backfill.csv';

COPY INTO STG.SRC_PHONESKILLSREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PhoneSkillsReason/PhoneSkillsReason_Backfill.csv';

COPY INTO STG.SRC_RULEDEFEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RuleDefEdit/RuleDefEdit_Backfill.csv';

-- COPY INTO STG.SRC_PTPPAYMENTPLANPAYMENTMETHOD_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/PTPPaymentPlanPaymentMethod/PTPPaymentPlanPaymentMethod_Backfill.csv';

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

COPY INTO STG.SRC_INCOMEVERIFYMETHODLOCATIONHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerifyMethodLocationHistory/IncomeVerifyMethodLocationHistory_Backfill.csv';

COPY INTO STG.SRC_SPECIALMESSAGEHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $15 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SpecialMessageHistory/SpecialMessageHistory_Backfill.csv';

COPY INTO STG.SRC_EXTERNALAPPS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExternalApps/ExternalApps_Backfill.csv';

COPY INTO STG.SRC_WEBLEADGENTIERS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadGenTiers/WebLeadGenTiers_Backfill.csv';

-- COPY INTO STG.SRC_GLOBALSTATES_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, $6, $7, $8, 
-- case when $9 = 'True' then 1 else 0 end, $10, 
-- case when $11 = 'True' then 1 else 0 end, 
-- case when $12 = 'True' then 1 else 0 end, $13, 
-- case when $14 = 'True' then 1 else 0 end, 
-- case when $15 = 'True' then 1 else 0 end, $16, $17, 
-- case when $18 = 'True' then 1 else 0 end, 
-- case when $19 = 'True' then 1 else 0 end, $20, $21, 
-- case when $22 = 'True' then 1 else 0 end, $23, $24, $25, $26
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/GlobalStates/GlobalStates_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLRARRCATEGORYREASON_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRARRCategoryReason/WebCallRARRCategoryReason_Backfill.csv';

COPY INTO STG.SRC_TOTALDAILYFEES2_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TotalDailyFees2/TotalDailyFees2_Backfill.csv';

COPY INTO STG.SRC_CALLCAMPAIGNHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when contains($5, '-') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, 
case when contains($6, '-') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, 
case when contains($7, '-') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, $8, 
case when $9 = 'True' then 1 else 0 end, $10, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignHistory/CallCampaignHistory_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLRARREASON_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when contains($5, '+') = True then try_to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else try_to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRARReason/WebCallRARReason_Backfill.csv';

COPY INTO STG.SRC_DOCUWARECABINET_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11, 
case when $12 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareCabinet/DocuwareCabinet_Backfill.csv';

COPY INTO STG.SRC_INCOMETYPELOCATIONHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeTypeLocationHistory/IncomeTypeLocationHistory_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSEMAILLOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusEmailLocation/OptPlusEmailLocation_Backfill.csv';

-- COPY INTO STG.SRC_SPECIALMESSAGE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, 
-- case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, 
-- case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8, 
-- case when $9 = 'True' then 1 else 0 end, 
-- case when $10 = 'True' then 1 else 0 end, 
-- case when $11 = 'True' then 1 else 0 end, 
-- case when $12 = 'True' then 1 else 0 end, $13, $14, $15
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/SpecialMessage/SpecialMessage_Backfill.csv';

-- COPY INTO STG.SRC_CREDITREPORTINGLOANPRODUCTLOCATION_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
-- case when $7 = 'True' then 1 else 0 end, $8, $9
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/CreditReportingLoanProductLocation/CreditReportingLoanProductLocation_Backfill.csv';

COPY INTO STG.SRC_BILLPAYBILLER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11, 
case when $12 = '' then null else $12 end, 
case when $13 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BillPayBiller/BillPayBiller_Backfill.csv';

COPY INTO STG.SRC_EXCHANGERATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExchangeRate/ExchangeRate_Backfill.csv';

COPY INTO STG.SRC_REDACTEDWORDS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RedactedWords/RedactedWords_Backfill.csv';

COPY INTO STG.SRC_DENOMINATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else $7 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Denomination/Denomination_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLRARRESULT2_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRARResult2/WebCallRARResult2_Backfill.csv';

COPY INTO STG.SRC_ACHREQUEST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHRequest/ACHRequest_Backfill.csv';

COPY INTO STG.SRC_ACHQUEUE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, $5, $6, $7, $8, $9, 
case when $10 = 'True' then 1 else 0 end, $11, 
case when $12 = '' then null else $12 end, 
case when $13 = '' then null else $13 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHQueue/ACHQueue_Backfill.csv';

COPY INTO STG.SRC_CREDITCARDRESULTCODEEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when contains($7, '+') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardResultCodeEdit/CreditCardResultCodeEdit_Backfill.csv';

COPY INTO STG.SRC_AUTOREPORTSCHEDULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AutoReportSchedule/AutoReportSchedule_Backfill.csv';

COPY INTO STG.SRC_DIALERJOBALLOWEDAUTODIALTCPARESULT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerJobAllowedAutoDialTCPAResult/DialerJobAllowedAutoDialTCPAResult_Backfill.csv';

COPY INTO STG.SRC_ESIGNLOANDOC_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else $6 end, 
case when $7 = 'True' then 1 else 0 end, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ESignLoanDoc/ESignLoanDoc_Backfill.csv';

COPY INTO STG.SRC_RIPTPPAYMENTPLANCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, $15, $16, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = 'True' then 1 else 0 end, $20, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end, 
case when $23 = 'True' then 1 else 0 end, 
case when $24 = 'True' then 1 else 0 end, $25, 
case when $26 = 'True' then 1 else 0 end, 
case when $27 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RIPTPPaymentPlanConfig/RIPTPPaymentPlanConfig_Backfill.csv';

COPY INTO STG.SRC_AGENTACTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AgentAction/AgentAction_Backfill.csv';

-- COPY INTO STG.SRC_SECURITYGROUP_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when $6 = 'True' then 1 else 0 end, 
-- case when $7 = 'True' then 1 else 0 end, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, $10, $11, $12
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/SecurityGroup/SecurityGroup_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGTARRACTIVITYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingTarrActivityXref/CreditReportingTarrActivityXref_Backfill.csv';

COPY INTO STG.SRC_DEBTSALEEXPORTLOG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = 'True' then 1 else 0 end, 
case when contains($3, '+') = True then to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DebtSaleExportLog/DebtSaleExportLog_Backfill.csv';

COPY INTO STG.SRC_DIALERJOBRISAUDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerJobRisAudit/DialerJobRisAudit_Backfill.csv';

COPY INTO STG.SRC_RITAPWD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RitaPwd/RitaPwd_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLCATEGORY_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when $6 = '' then null else $6 end, $7, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, 
-- case when $10 = 'True' then 1 else 0 end, 
-- case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when contains($12, '+') = True then to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
-- case when contains($13, '+') = True then to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else try_to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $14, $15
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallCategory/WebCallCategory_Backfill.csv';

COPY INTO STG.SRC_INCOMEVERIFICATIONSOURCELOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerificationSourceLocation/IncomeVerificationSourceLocation_Backfill.csv';

COPY INTO STG.SRC_IATDIALERRESULTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IatDialerResults/IatDialerResults_Backfill.csv';

-- COPY INTO STG.SRC_FUNDINGMETHODLOCATIONXREFCONFIG_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/FundingMethodLocationXRefConfig/FundingMethodLocationXRefConfig_Backfill.csv';

COPY INTO STG.SRC_LOANPAYMENTRESCIND_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentRescind/LoanPaymentRescind_Backfill.csv';

COPY INTO STG.SRC_MESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Message/Message_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCATRARRALIAS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCatRarrAlias/WebCallCatRarrAlias_Backfill.csv';

COPY INTO STG.SRC_VISITORPREQUALIFICATIONXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorPreQualificationXRef/VisitorPreQualificationXRef_Backfill.csv';

COPY INTO STG.SRC_MONEYORDERPRINTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyOrderPrinter/MoneyOrderPrinter_Backfill.csv';

COPY INTO STG.SRC_COUNTRY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = 'True' then 1 else 0 end, $6, 
case when $7 = '' then null else $7 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Country/Country_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACESTEP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = 'True' then 1 else 0 end, 
case when $16 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceStep/SkipTraceStep_Backfill.csv';

COPY INTO STG.SRC_COLLECTIONAGINGCONFIGDAYS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionAgingConfigDays/CollectionAgingConfigDays_Backfill.csv';

COPY INTO STG.SRC_MSA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MSA/MSA_Backfill.csv';

COPY INTO STG.SRC_ESIGNOPTIN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ESignOptIn/ESignOptIn_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = 'True' then 1 else 0 end, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProduct/LoanProduct_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLEMAILTEMPLATES_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, 
-- case when $6 = 'True' then 1 else 0 end, 
-- case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, $10, $11, $12
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallEmailTemplates/WebCallEmailTemplates_Backfill.csv';

COPY INTO STG.SRC_AGENTRESULT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AgentResult/AgentResult_Backfill.csv';

COPY INTO STG.SRC_DRAWERSERVICE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerService/DrawerService_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERFLASHMPAYREBATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFlashMPayRebates/CustomerFlashMPayRebates_Backfill.csv';

COPY INTO STG.SRC_CUSTOMERLEADLOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, $6, 
case when $7 = '' then null else $7 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadLocation/CustomerLeadLocation_Backfill.csv';

-- COPY INTO STG.SRC_DRAWERMASTER_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
-- case when $5 = '' then null else $5 end, $6, $7, $8, $9, $10, 
-- case when $11 = 'True' then 1 else 0 end, $12, $13, $14, $15, $16, 
-- case when $17 = '' then $17 end
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/DrawerMaster/DrawerMaster_Backfill.csv';

COPY INTO STG.SRC_TESTCREDITCARD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TestCreditCard/TestCreditCard_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSEXPORTINITGL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusExportInitGL/OptPlusExportInitGL_Backfill.csv';

-- COPY INTO STG.SRC_PTPPAYMENTPLANCHECK_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/PTPPaymentPlanCheck/PTPPaymentPlanCheck_Backfill.csv';

COPY INTO STG.SRC_SG_RIGHTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SG_RIGHTS/SG_RIGHTS_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTCONFIGANNUALRATEBANDLOANAMTRANGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigAnnualRateBandLoanAmtRange/LoanProductConfigAnnualRateBandLoanAmtRange_Backfill.csv';

-- COPY INTO STG.SRC_PTPPAYMENTPLANCONFIG_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, 
-- case when $3 = 'True' then 1 else 0 end, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
-- case when $13 = 'True' then 1 else 0 end, 
-- case when $14 = 'True' then 1 else 0 end, $15, $16, $17, $18, $19, $20, 
-- case when $21 = 'True' then 1 else 0 end, 
-- case when $22 = 'True' then 1 else 0 end, 
-- case when $23 = 'True' then 1 else 0 end, 
-- case when $24 = 'True' then 1 else 0 end, 
-- case when $25 = 'True' then 1 else 0 end, 
-- case when $26 = 'True' then 1 else 0 end, 
-- case when $27 = 'True' then 1 else 0 end, $28, $29, 
-- case when $30 = 'True' then 1 else 0 end, 
-- case when $31 = 'True' then 1 else 0 end, 
-- case when $32 = 'True' then 1 else 0 end, $33, $34, $35, 
-- case when $36 = 'True' then 1 else 0 end, 
-- case when $37 = 'True' then 1 else 0 end, 
-- case when $38 = 'True' then 1 else 0 end, $39, $40, 
-- case when contains($41, '+') = True then to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $42, 
-- case when $43 = '' then null when contains($43, '+') = True then to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $44, 
-- case when $45 = 'True' then 1 else 0 end, $46, $47, $48
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/PTPPaymentPlanConfig/PTPPaymentPlanConfig_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterProduct/FormLetterProduct_Backfill.csv';

COPY INTO STG.SRC_BANKCLASSIFICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankClassification/BankClassification_Backfill.csv';

-- COPY INTO STG.SRC_COMPANYDETAIL_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/CompanyDetail/CompanyDetail_Backfill.csv';

-- COPY INTO STG.SRC_ACH_RETURNCODE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, 
-- case when $6 = 'True' then 1 else 0 end, 
-- case when $7 = 'True' then 1 else 0 end, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, 
-- case when $10 = 'True' then 1 else 0 end, 
-- case when $11 = '' then null else $11 end, $12, $13
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/ACH_ReturnCode/ACH_ReturnCode_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSRDFLOOKUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusRDFLookUp/OptPlusRDFLookUp_Backfill.csv';

COPY INTO STG.SRC__TMPLOCATIONFUNDINGMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_tmpLocationFundingMethod/_tmpLocationFundingMethod_Backfill.csv';

COPY INTO STG.SRC_RISAUDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = 'True' then 1 else 0 end, 
case when $16 = 'True' then 1 else 0 end, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = 'True' then 1 else 0 end, 
case when $20 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RISAUDIT/RISAUDIT_Backfill.csv';

-- COPY INTO STG.SRC_WEBCALLRARRESULT1_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/WebCallRARResult1/WebCallRARResult1_Backfill.csv';

COPY INTO STG.SRC__CH022850STATEMENTFIXES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $16 = '' then null else to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $18 = '' then null else to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:SS AM') end, $19, $20, $21, $22, 
case when $23 = '' then null else to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $24 = '' then null else to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $25 = '' then null else to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $26 = '' then null else to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CH022850StatementFixes/_CH022850StatementFixes_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTCONFIGAPPROVALRATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigApprovalRate/LoanProductConfigApprovalRate_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGLOANPRODUCTLOCATIONHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanProductLocationHistory/CreditReportingLoanProductLocationHistory_Backfill.csv';

COPY INTO STG.SRC_ESIGNLOAN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
TO_BINARY(HEX_ENCODE($2), 'HEX'), $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ESignLoan/ESignLoan_Backfill.csv';

-- COPY INTO STG.SRC_PTPPAYMENTPLANSECURITYGROUP_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/PTPPaymentPlanSecurityGroup/PTPPaymentPlanSecurityGroup_Backfill.csv';

COPY INTO STG.SRC_PROCESSSCHEDULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, $11, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, $15, 
case when $16 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessSchedule/ProcessSchedule_Backfill.csv';

COPY INTO STG.SRC_TOTALDAILYFEES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TotalDailyFees/TotalDailyFees_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONEVENTQUEUE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else $4 end, $5, $6, $7, 
case when contains($8, '+') = True then to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, $12, 
case when $13 = '' then null else $13 end, 
case when $14 = '' then null else $14 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationEventQueue/CommunicationEventQueue_Backfill.csv';

COPY INTO STG.SRC_DIALERJOB_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = 'True' then 1 else 0 end, $8, $9, $10, $11, $12, $13, $14, $15, 
case when $16 = 'True' then 1 else 0 end, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = 'True' then 1 else 0 end, 
case when $20 = 'True' then 1 else 0 end, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end, 
case when $23 = 'True' then 1 else 0 end, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end, 
case when $26 = 'True' then 1 else 0 end, 
case when $27 = 'True' then 1 else 0 end, 
case when $28 = 'True' then 1 else 0 end, $29, $30, $31, 
case when $32 = 'True' then 1 else 0 end, $33, $34, $35, 
case when $36 = 'True' then 1 else 0 end, 
case when $37 = 'True' then 1 else 0 end, 
case when $38 = 'True' then 1 else 0 end, $39, $40
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerJob/DialerJob_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACESTEP_AUDITCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceStep_AuditCategory/SkipTraceStep_AuditCategory_Backfill.csv';

COPY INTO STG.SRC_GOLDTRANSSTONEDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldTransStoneDetail/GoldTransStoneDetail_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGRUN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when contains($3, '-') = True then to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, 
case when contains($4, '-') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, $5, $6, 
case when $7 = 'True' then 1 else 0 end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingRun/CreditReportingRun_Backfill.csv';

COPY INTO STG.SRC_ATTORNEYTYPEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AttorneyTypeXRef/AttorneyTypeXRef_Backfill.csv';

COPY INTO STG.SRC_CAPSCCTXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CapsCCTXRef/CapsCCTXRef_Backfill.csv';

COPY INTO STG.SRC_SERVICEMASTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, $15, 
case when $16 = 'True' then 1 else 0 end, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = 'True' then 1 else 0 end, 
case when $20 = 'True' then 1 else 0 end, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end, 
case when $23 = 'True' then 1 else 0 end, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ServiceMaster/ServiceMaster_Backfill.csv';

COPY INTO STG.SRC_AMLFOREIGNADDRESS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLForeignAddress/AMLForeignAddress_Backfill.csv';

COPY INTO STG.SRC_DISCOUNTMASTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else $2 end, $3, $4, $5, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, $10, $11, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end, $14, 
case when $15 = 'True' then 1 else 0 end, 
case when $16 = 'True' then 1 else 0 end, $17, 
case when $18 = '' then null else $18 end, $19, 
case when $20 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DiscountMaster/DiscountMaster_Backfill.csv';

COPY INTO STG.SRC_DISCOUNTSECURITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DiscountSecurity/DiscountSecurity_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTINTERNETZIPCODEEXCLUSION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductInternetZipCodeExclusion/LoanProductInternetZipCodeExclusion_Backfill.csv';

COPY INTO STG.SRC_DBPURGETABLES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DbPurgeTables/DbPurgeTables_Backfill.csv';

COPY INTO STG.SRC_LOCATIONTRANSACTIONPROCESSOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LocationTransactionProcessor/LocationTransactionProcessor_Backfill.csv';

COPY INTO STG.SRC_VAULTSERVICE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultService/VaultService_Backfill.csv';

-- COPY INTO STG.SRC_IDENTIFICATIONTYPESTATE_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = 'True' then 1 else 0 end, 
-- case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
-- case when $7 = 'True' then 1 else 0 end, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, 
-- case when $10 = 'True' then 1 else 0 end, 
-- case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, $12, $13, $14
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/IdentificationTypeState/IdentificationTypeState_Backfill.csv';

COPY INTO STG.SRC_OUTOFWALLETQUIZLOANAPPLICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletQuizLoanApplication/OutOfWalletQuizLoanApplication_Backfill.csv';

COPY INTO STG.SRC_MONEYORDERPRINTERSTOREWINDOWS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyOrderPrinterStoreWindows/MoneyOrderPrinterStoreWindows_Backfill.csv';

COPY INTO STG.SRC_WEBCALLINVALIDPHONENUMBER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallInvalidPhoneNumber/WebCallInvalidPhoneNumber_Backfill.csv';

COPY INTO STG.SRC_AUTOREPORTTAB_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AutoReportTab/AutoReportTab_Backfill.csv';

COPY INTO STG.SRC_CREDITLIMITOFFERDECLINE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when contains($2, '+') = True then to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditLimitOfferDecline/CreditLimitOfferDecline_Backfill.csv';

-- COPY INTO STG.SRC_GLACCTLOANPRODUCTGROUP_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/GLAcctLoanProductGroup/GLAcctLoanProductGroup_Backfill.csv';

COPY INTO STG.SRC_SPECIALMESSAGE_AZCUSTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SpecialMessage_AzCusts/SpecialMessage_AzCusts_Backfill.csv';

COPY INTO STG.SRC_FORMLETTERAUDITCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterAuditCode/FormLetterAuditCode_Backfill.csv';

COPY INTO STG.SRC_INITGLLISTHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InitGLListHistory/InitGLListHistory_Backfill.csv';

COPY INTO STG.SRC_VAULTMASTERPARSEDCASH_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultMasterParsedCash/VaultMasterParsedCash_Backfill.csv';

COPY INTO STG.SRC_AUTOREPORTRUNSCHEDULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $15 = 'True' then 1 else 0 end, 
case when $16 = '' then null else $16 end, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:SS AM') end, $18
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AutoReportRunSchedule/AutoReportRunSchedule_Backfill.csv';

COPY INTO STG.SRC_LOANAUTHORIZEDPAYMENTMETHODPENDING_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end, $4, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null else $9 end, 
case when $10 = '' then null else $10 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanAuthorizedPaymentMethodPending/LoanAuthorizedPaymentMethodPending_Backfill.csv';

COPY INTO STG.SRC_ACHLOANPAYMENTREFUND_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else $2 end, 
case when $3 = '' then null else $3 end, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHLoanPaymentRefund/ACHLoanPaymentRefund_Backfill.csv';

-- COPY INTO STG.SRC_TRANSACTIONPROCESSORCOMPANYBANKACCOUNTCONFIG_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = '' then null else $4 end, 
-- case when $5 = '' then null else $5 end, 
-- case when $6 = 'True' then 1 else 0 end, $7, $8, $9, $10, $11, $12
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/TransactionProcessorCompanyBankAccountConfig/TransactionProcessorCompanyBankAccountConfig_Backfill.csv';

COPY INTO STG.SRC_TELLERSECURITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = 'True' then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerSecurity/TellerSecurity_Backfill.csv';

COPY INTO STG.SRC_ACH_RECV_FIX_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ach_recv_fix/ach_recv_fix_Backfill.csv';

COPY INTO STG.SRC_AUTOREPORTEMAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AutoReportEmail/AutoReportEmail_Backfill.csv';

COPY INTO STG.SRC__FORMLETTERRESULT_200103_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_FormLetterResult_200103/_FormLetterResult_200103_Backfill.csv';

COPY INTO STG.SRC_VISITORPREQUALIFICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when contains($2, '-') = True then to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else $7 end, 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null when contains($9, '-') = True then to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY H12:MI:SS AM') end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = '' then null when contains($13, '-') = True then to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, $14, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorPreQualification/VisitorPreQualification_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSBINSERVICE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusBinService/OptPlusBinService_Backfill.csv';

-- COPY INTO STG.SRC_TRANSACTIONPROCESSORCOMPANYBANKACCOUNTCONFIGHISTORY_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, $4, 
-- case when $5 = '' then null else $5 end, 
-- case when $6 = 'True' then 1 else 0 end, $7, $8, $9, 
-- case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY H12:MI:SS PM') end, $11, $12
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/TransactionProcessorCompanyBankAccountConfigHistory/TransactionProcessorCompanyBankAccountConfigHistory_Backfill.csv';

COPY INTO STG.SRC_LOANAPPLICATIONPENDINGREASONCONFIGURABLEQUESTIONRESPONSE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationPendingReasonConfigurableQuestionResponse/LoanApplicationPendingReasonConfigurableQuestionResponse_Backfill.csv';

-- COPY INTO STG.SRC_PREPAIDCARDBINCOMPANY_HIST FROM
-- (select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
-- $1, $2, $3, 
-- case when $4 = '' then null else $4 end, 
-- case when $5 = 'True' then 1 else 0 end, 
-- case when $6 = '' then null when $6 = 'True' then 1 else 0 end, 
-- case when $7 = 'True' then 1 else 0 end, 
-- case when $8 = 'True' then 1 else 0 end, 
-- case when $9 = 'True' then 1 else 0 end, 
-- case when $10 = 'True' then 1 else 0 end, $11, $12, 
-- case when $13 = 'True' then 1 else 0 end, 
-- case when $14 = 'True' then 1 else 0 end, $15, $16
-- from @ETL.INBOUND)
-- FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
-- pattern= 'inbound/SRC/Backfill/PrepaidCardBinCompany/PrepaidCardBinCompany_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, $15, $16, $17, $18, $19, $20, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end, 
case when $23 = 'True' then 1 else 0 end, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end, 
case when $26 = 'True' then 1 else 0 end, 
case when $27 = 'True' then 1 else 0 end, $28, $29, 
case when $30 = 'True' then 1 else 0 end, 
case when $31 = 'True' then 1 else 0 end, 
case when $32 = 'True' then 1 else 0 end, $33, $34, $35, 
case when $36 = 'True' then 1 else 0 end, 
case when $37 = 'True' then 1 else 0 end, 
case when $38 = 'True' then 1 else 0 end, $39, $40, 
case when contains($41, '+') = True then to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end , $42, 
case when $43 = '' then null when contains($43, '+') = True then to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end , $44, 
case when $45 = 'True' then 1 else 0 end, 
case when $46 = '' then null else to_timestamp_ntz($46, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $47 = '' then null else to_timestamp_ntz($47, 'MM/DD/YYYY HH12:MI:SS AM') end, $48
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanConfigHistory/PTPPaymentPlanConfigHistory_Backfill.csv';

COPY INTO STG.SRC__FORMLETTERONDEMAND_200103_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8, $9, $10, $11, 
case when $12 = '' then null else $12 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_FormLetterOnDemand_200103/_FormLetterOnDemand_200103_Backfill.csv';

COPY INTO STG.SRC_GOLDCONFIGITEM_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldConfigItem/GoldConfigItem_Backfill.csv';

COPY INTO STG.SRC_GOLDCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end, 
case when $26 = 'True' then 1 else 0 end, 
case when $27 = 'True' then 1 else 0 end, 
case when $28 = 'True' then 1 else 0 end, 
case when $29 = 'True' then 1 else 0 end, 
case when $30 = 'True' then 1 else 0 end, 
case when $31 = 'True' then 1 else 0 end, $32, 
case when $33 = 'True' then 1 else 0 end, 
case when $34 = 'True' then 1 else 0 end, 
case when $35 = 'True' then 1 else 0 end, 
case when $36 = 'True' then 1 else 0 end, $37, 
case when $38 = 'True' then 1 else 0 end, 
case when $39 = 'True' then 1 else 0 end, $40, 
case when $41 = 'True' then 1 else 0 end, $42, $43, 
case when $44 = 'True' then 1 else 0 end, $45, 
case when $46 = '' then null else to_timestamp_ntz($46, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $47 = '' then null else to_timestamp_ntz($47, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldConfigHistory/GoldConfigHistory_Backfill.csv';

COPY INTO STG.SRC_DOCUWARELOANLKUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareLoanLkup/DocuwareLoanLkup_Backfill.csv';

COPY INTO STG.SRC__FORMLETTERPRINTED_200103_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, $5, $6, 
case when $7 = '' then null else $7 end, 
case when $8 = '' then null else $8 end, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11, 
case when $12 = '' then null else $12 end, 
case when $13 = '' then null else $13 end, 
case when $14 = '' then null else $14 end, $15, 
case when $16 = '' then null else $16 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_FormLetterPrinted_200103/_FormLetterPrinted_200103_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARRType/WebCallRARRType_Backfill.csv';

COPY INTO STG.SRC_NOBLECONFIGURATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else $6 end, $7, $8, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/NobleConfiguration/NobleConfiguration_Backfill.csv';

COPY INTO STG.SRC_LENDER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Lender/Lender_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACECONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = 'True' then 1 else 0 end, $3, $4, $5, $6, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceConfig/SkipTraceConfig_Backfill.csv';

COPY INTO STG.SRC_INCOMETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeType/IncomeType_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHODGROUPCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethodGroupConfig/FundingMethodGroupConfig_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGRULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingRule/CreditReportingRule_Backfill.csv';

COPY INTO STG.SRC_GLACCTGLOBAL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, 
case when $32 = '' then null else $32 end, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctGlobal/GLAcctGlobal_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, 
case when contains($6, '+') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRarrGroup/WebCallRarrGroup_Backfill.csv';

COPY INTO STG.SRC_SKIPTRACEVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, $7, $8, $9, $10, $11, $12, $13, $14, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceVendor/SkipTraceVendor_Backfill.csv';

COPY INTO STG.SRC_VISITORDOCUMENTTEMPLATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
TO_BINARY(HEX_ENCODE($6), 'HEX'), $7, $8, 
case when $9 = 'True' then 1 else 0 end, $10, 
case when contains($11, '+') = True then to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDocumentTemplate/VisitorDocumentTemplate_Backfill.csv';

COPY INTO STG.SRC_INSURANCESTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceStatus/InsuranceStatus_Backfill.csv';

COPY INTO STG.SRC_INCOMESOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeSource/IncomeSource_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHODGROUPITEMCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethodGroupItemConfig/FundingMethodGroupItemConfig_Backfill.csv';

COPY INTO STG.SRC_ACCUMCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AccumConfig/AccumConfig_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANCONFIGDOCUMENTXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanConfigDocumentXRef/PTPPaymentPlanConfigDocumentXRef_Backfill.csv';

COPY INTO STG.SRC_GLACCTLOCATIONGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctLocationGroup/GLAcctLocationGroup_Backfill.csv';

COPY INTO STG.SRC_INCOMEVERIFYMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerifyMethod/IncomeVerifyMethod_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
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
case when $17 = 'True' then 1 else 0 end, $18, $19
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationType/IdentificationType_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONCONSENTCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else $7 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationConsentConfig/CommunicationConsentConfig_Backfill.csv';

COPY INTO STG.SRC_OPTPLUSCARRIER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, $9, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusCarrier/OptPlusCarrier_Backfill.csv';

COPY INTO STG.SRC_WEBCALLFEATURES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when contains($6, '+') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallFeatures/WebCallFeatures_Backfill.csv';

COPY INTO STG.SRC_VISITORAUTHENTICATIONCODECONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else $2 end, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $8 = '' then null else $8 end, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorAuthenticationCodeConfig/VisitorAuthenticationCodeConfig_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRACTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARRAction/WebCallRARRAction_Backfill.csv';

COPY INTO STG.SRC_CALLCAMPAIGN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when contains($6, '+') = True then to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when contains($7, '+') = True then to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $8, 
case when $9 = 'True' then 1 else 0 end, $10, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, $13, $14
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaign/CallCampaign_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANPAYMENTMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanPaymentMethod/PTPPaymentPlanPaymentMethod_Backfill.csv';

COPY INTO STG.SRC_GLOBALSTATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, $10, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, $13, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = 'True' then 1 else 0 end, $16, $17, 
case when $18 = 'True' then 1 else 0 end, 
case when $19 = 'True' then 1 else 0 end, $20, $21, 
case when $22 = 'True' then 1 else 0 end, $23, $24, $25, $26
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GlobalStates/GlobalStates_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRCATEGORYREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARRCategoryReason/WebCallRARRCategoryReason_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARReason/WebCallRARReason_Backfill.csv';

COPY INTO STG.SRC_SPECIALMESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, $7, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = 'True' then 1 else 0 end, 
case when $13 = 'True' then 1 else 0 end, $14, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SpecialMessage/SpecialMessage_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRESULT2_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARResult2/WebCallRARResult2_Backfill.csv';

COPY INTO STG.SRC_SECURITYGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SecurityGroup/SecurityGroup_Backfill.csv';

COPY INTO STG.SRC_FUNDINGMETHODLOCATIONXREFCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FundingMethodLocationXRefConfig/FundingMethodLocationXRefConfig_Backfill.csv';

COPY INTO STG.SRC_WEBCALLEMAILTEMPLATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, $10, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallEmailTemplates/WebCallEmailTemplates_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANCHECK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanCheck/PTPPaymentPlanCheck_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, $15, $16, $17, $18, $19, $20, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end, 
case when $23 = 'True' then 1 else 0 end, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end, 
case when $26 = 'True' then 1 else 0 end, 
case when $27 = 'True' then 1 else 0 end, $28, $29, 
case when $30 = 'True' then 1 else 0 end, 
case when $31 = 'True' then 1 else 0 end, 
case when $32 = 'True' then 1 else 0 end, $33, $34, $35, 
case when $36 = 'True' then 1 else 0 end, 
case when $37 = 'True' then 1 else 0 end, 
case when $38 = 'True' then 1 else 0 end, $39, $40, 
case when contains($41, '+') = True then to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $42, 
case when $43 = '' then null when contains($43, '+') = True then to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $44, 
case when $45 = 'True' then 1 else 0 end, $46, $47, $48
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanConfig/PTPPaymentPlanConfig_Backfill.csv';

COPY INTO STG.SRC_COMPANYDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyDetail/CompanyDetail_Backfill.csv';

COPY INTO STG.SRC_ACH_RETURNCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, 
case when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = '' then null else $11 end, $12, $13
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACH_ReturnCode/ACH_ReturnCode_Backfill.csv';

COPY INTO STG.SRC_WEBCALLRARRESULT1_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when contains($5, '+') = True then to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARResult1/WebCallRARResult1_Backfill.csv';

COPY INTO STG.SRC_PTPPAYMENTPLANSECURITYGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanSecurityGroup/PTPPaymentPlanSecurityGroup_Backfill.csv';

COPY INTO STG.SRC_IDENTIFICATIONTYPESTATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, $12, $13, $14
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeState/IdentificationTypeState_Backfill.csv';

COPY INTO STG.SRC_GLACCTLOANPRODUCTGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctLoanProductGroup/GLAcctLoanProductGroup_Backfill.csv';

COPY INTO STG.SRC_TRANSACTIONPROCESSORCOMPANYBANKACCOUNTCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else $5 end, 
case when $6 = 'True' then 1 else 0 end, $7, $8, $9, 
case when $10 = 'True' then 1 else 0 end, $11, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionProcessorCompanyBankAccountConfig/TransactionProcessorCompanyBankAccountConfig_Backfill.csv';

COPY INTO STG.SRC_PREPAIDCARDBINCOMPANY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null when $6 = 'True' then 1 else 0 end, 
case when $7 = 'True' then 1 else 0 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, $11, $12, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = 'True' then 1 else 0 end, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrepaidCardBinCompany/PrepaidCardBinCompany_Backfill.csv';

COPY INTO STG.SRC_WEBALERT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebAlert/WebAlert_Backfill.csv';

COPY INTO STG.SRC_DRAWERMASTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else $5 end, $6, $7, $8, $9, $10, 
case when $11 = 'True' then 1 else 0 end, $12, $13, $14, $15, $16, 
case when $17 = '' then null else $17 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerMaster/DrawerMaster_Backfill.csv';

COPY INTO STG.SRC_TRANSACTIONPROCESSORCOMPANYBANKACCOUNTCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = 'True' then 1 else 0 end, $7, $8, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end,
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $12 = 'True' then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionProcessorCompanyBankAccountConfigHistory/TransactionProcessorCompanyBankAccountConfigHistory_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGLOANPRODUCTLOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'),
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = 'True' then 1 else 0 end, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanProductLocation/CreditReportingLoanProductLocation_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else $6 end, $7, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when contains($12, '+') = True then to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
$13, $14, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCategory/WebCallCategory_Backfill.csv';

COPY INTO STG.SRC_GLOBAL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-04'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, $11, $12, $13, $14, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, $16, $17, $18, $19, $20, $21, $22, $23, 
case when $24 = 'True' then 1 else 0 end, 
case when $25 = 'True' then 1 else 0 end, 
case when $26 = 'True' then 1 else 0 end, 
case when $27 = 'True' then 1 else 0 end, 
case when $28 = 'True' then 1 else 0 end, 
case when $29 = 'True' then 1 else 0 end, 
case when $30 = 'True' then 1 else 0 end, 
case when $31 = 'True' then 1 else 0 end, $32, $33, $34, $35, $36, $37, $38, $39, $40, 
case when $41 = 'True' then 1 else 0 end, $42, $43, 
case when $44 = '' then null else to_timestamp_ntz($44, 'MM/DD/YYYY HH12:MI:SS AM') end, $45, $46, 
case when $47 = '' then null else to_timestamp_ntz($47, 'MM/DD/YYYY HH12:MI:SS AM') end, $48, $49, 
case when $50 = 'True' then 1 else 0 end, $51, $52, 
case when $53 = '' then null else to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $54 = '' then null else to_timestamp_ntz($54, 'MM/DD/YYYY HH12MI:SS AM') end, $55, $56, $57, $58, $59, 
case when $60 = 'True' then 1 else 0 end, $61, $62, 
case when $63 = 'True' then 1 else 0 end, $64, 
case when $65 = 'True' then 1 else 0 end, $66, $67, $68, 
case when $69 = 'True' then 1 else 0 end, $70, $71, 
case when $72 = 'True' then 1 else 0 end, 
case when $73 = 'True' then 1 else 0 end, $74, $75, $76, 
case when $77 = 'True' then 1 else 0 end, 
case when $78 = 'True' then 1 else 0 end, $79, 
case when $80 = 'True' then 1 else 0 end, 
case when $81 = 'True' then 1 else 0 end, $82, $83, $84, $85, 
case when $86 = 'True' then 1 else 0 end, 
case when $87 = 'True' then 1 else 0 end, $88, $89, 
case when $90 = 'True' then 1 else 0 end, 
case when $91 = 'True' then 1 else 0 end, $92, 
case when $93 = 'True' then 1 else 0 end, $94, 
case when $95 = 'True' then 1 else 0 end, $96, $97, $98, $99, $100, $101
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Global/Global_Backfill.csv';
