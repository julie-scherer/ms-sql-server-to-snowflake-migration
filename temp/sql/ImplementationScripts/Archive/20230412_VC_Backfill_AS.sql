COPY INTO STG.VC_COLLECTIONAGENCY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer),
$2, $3, $4, $5, $6, $7, $8, $9, $10, 
case when $11 = True then 1 else 0 end, $12, $13, $14, $15, $16, case when $17 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionAgency/CollectionAgency_Backfill.csv';

COPY INTO STG.VC_COLLECTIONSETTLEMENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer),
$2, 
case when $3 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionSettlementReason/CollectionSettlementReason_Backfill.csv';

COPY INTO STG.VC_COMMUNICATIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer),
$2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationType/CommunicationType_Backfill.csv';

COPY INTO STG.VC_CREDITCARDVENDORCONFIGEXCEPTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer),
$2, 
case when $3 = '' then NULL else $3 end, 
case when $4 = '' then NULL else $4 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardVendorConfigException/CreditCardVendorConfigException_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGDISPUTESOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer),
$2, 
CASE WHEN $3 = TRUE THEN 1 ELSE 0 END
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingDisputeSource/CreditReportingDisputeSource_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGRUNSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingRunStatus/CreditReportingRunStatus_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingStatus/CreditReportingStatus_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGTRANSCODEACTIVITYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingTransCodeActivityXRef/CreditReportingTransCodeActivityXRef_Backfill.csv';

COPY INTO STG.VC_CUROHELP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CuroHelp/CuroHelp_Backfill.csv';

COPY INTO STG.VC_CURRENCY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, 
case when $7 = True then 1 else 0 end, $8, $9, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Currency/Currency_Backfill.csv';

COPY INTO STG.VC_DIALERJOBALLOWEDAUTODIALTCPARESULT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DialerJobAllowedAutoDialTCPAResult/DialerJobAllowedAutoDialTCPAResult_Backfill.csv';

COPY INTO STG.VC_FORMLETTERBATCHSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterBatchStatus/FormLetterBatchStatus_Backfill.csv';

COPY INTO STG.VC_GLACCTGLOBAL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $9, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, 
case when $32 = '' then NULL else $32 end, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GLAcctGlobal/GLAcctGlobal_Backfill.csv';

COPY INTO STG.VC_HAIRCOLOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/HairColor/HairColor_Backfill.csv';

COPY INTO STG.VC_HOLIDAY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Holiday/Holiday_Backfill.csv';

COPY INTO STG.VC_IDENTIFICATIONTYPECATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IdentificationTypeCategory/IdentificationTypeCategory_Backfill.csv';

COPY INTO STG.VC_INCOMEVERIFICATIONSOURCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeVerificationSource/IncomeVerificationSource_Backfill.csv';

COPY INTO STG.VC_INCOMEVERIFYVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
try_to_timestamp($4)
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeVerifyVendor/IncomeVerifyVendor_Backfill.csv';

COPY INTO STG.VC_WSCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5, $6, 
case when $7 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WSConfig/WSConfig_Backfill.csv';

COPY INTO STG.VC_NOTETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/NoteType/NoteType_Backfill.csv';

COPY INTO STG.VC_CHECKPAYMENTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $5 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CheckPaymentType/CheckPaymentType_Backfill.csv';

COPY INTO STG.VC_EMAILDISPOSITION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EmailDisposition/EmailDisposition_Backfill.csv';

COPY INTO STG.VC_CREDITCARDVENDOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardVendor/CreditCardVendor_Backfill.csv';

COPY INTO STG.VC_ACHBANK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
case when $4 = True then 1 else 0 end, $5, 
case when $6 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHBank/ACHBank_Backfill.csv';

COPY INTO STG.VC_ASPECTDIMENSIONID_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AspectDimensionId/AspectDimensionId_Backfill.csv';

COPY INTO STG.VC_CAPSSKIPREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CapsSkipReason/CapsSkipReason_Backfill.csv';

COPY INTO STG.VC_INCOMEVERIFICATIONMESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), 
case when $2 = True then 1 else 0 end, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeVerificationMessage/IncomeVerificationMessage_Backfill.csv';

COPY INTO STG.VC_LOANPRODUCTFINANCIALGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductFinancialGroup/LoanProductFinancialGroup_Backfill.csv';

COPY INTO STG.VC_REFERRALMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ReferralMethod/ReferralMethod_Backfill.csv';

COPY INTO STG.VC_INCOMEVERIFYMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3,
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeVerifyMethod/IncomeVerifyMethod_Backfill.csv';

COPY INTO STG.VC_DIALERJOBRISAUDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DialerJobRisAudit/DialerJobRisAudit_Backfill.csv';

COPY INTO STG.VC_WEBCALLCHATCANNEDRESPONSES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallChatCannedResponses/WebCallChatCannedResponses_Backfill.csv';

COPY INTO STG.VC_FCRMAMLTRANSCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), $7, 
case when $8 = '' then NULL else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FcrmAmlTransCode/FcrmAmlTransCode_Backfill.csv';

COPY INTO STG.VC_VISITORNONMILITARYVERIFICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM'), $5, 
case when $6 = '' then NULL else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $7
-- case when $8 = '' then NULL else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorNonMilitaryVerification/VisitorNonMilitaryVerification_Backfill.csv';

COPY INTO STG.VC_RISTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RISTYPE/RISTYPE_Backfill.csv';

COPY INTO STG.VC_MARKETS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Markets/Markets_Backfill.csv';

COPY INTO STG.VC_TELLERCOMPUTER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerComputer/TellerComputer_Backfill.csv';

COPY INTO STG.VC_FCRMAMLTRANSCODEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FcrmAmlTransCodeXRef/FcrmAmlTransCodeXRef_Backfill.csv';

COPY INTO STG.VC_CUSTOMERLEADSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, $8, $9, 
case when $10 = True then 1 else 0 end, 
case when $11 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerLeadStatus/CustomerLeadStatus_Backfill.csv';

COPY INTO STG.VC_PTPPAYMENTPLANCHECK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanCheck/PTPPaymentPlanCheck_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGBASESEGMENTTAG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingBaseSegmentTag/CreditReportingBaseSegmentTag_Backfill.csv';

COPY INTO STG.VC_DIALERRESULTCODES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DialerResultCodes/DialerResultCodes_Backfill.csv';

COPY INTO STG.VC_RULEDEFSET_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6, 
case when $7 = '' then NULL else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RuleDefSet/RuleDefSet_Backfill.csv';

COPY INTO STG.VC_PRESENTMENTNOTSENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentNotSentReason/PresentmentNotSentReason_Backfill.csv';

COPY INTO STG.VC_VMATransType_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VMATransType/VMATransType_Backfill.csv';

COPY INTO STG.VC_COMMUNICATIONGROUP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationGroup/CommunicationGroup_Backfill.csv';

COPY INTO STG.VC_ACCUMCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7, $8, 
case when $9 = True then 1 else 0 end, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AccumConfig/AccumConfig_Backfill.csv';

COPY INTO STG.VC_VISITORAUTHENTICATIONCODECONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), 
case when $2 = '' then null else $2 end, 
case when $3 = True then 1 else 0 end, $4, $5, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorAuthenticationCodeConfig/VisitorAuthenticationCodeConfig_Backfill.csv';

COPY INTO STG.VC_CARDGOVERNOROVERRIDEHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CardGovernorOverrideHistory/CardGovernorOverrideHistory_Backfill.csv';

COPY INTO STG.VC_ASPECTADDONMETRICID_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AspectAddOnMetricId/AspectAddOnMetricId_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGDISPUTECODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingDisputeCode/CreditReportingDisputeCode_Backfill.csv';

COPY INTO STG.VC_PrescreenQuestion_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, $5, 
case when $6 = '' then null else $6 end, $7, $8, 
case when $9 = '' then null else $9 end, 
case when $10 = True then 1 else 0 end, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PrescreenQuestion/PrescreenQuestion_Backfill.csv';

COPY INTO STG.VC_ASPECTADDONMETRICIDDIMENSIONIDXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AspectAddOnMetricIdDimensionIdXref/AspectAddOnMetricIdDimensionIdXref_Backfill.csv';

COPY INTO STG.VC_RIPTPPAYMENTPLANCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = True then 1 else 0 end, 
case when $14 = True then 1 else 0 end, $15, $16, 
case when $17 = True then 1 else 0 end, 
case when $18 = True then 1 else 0 end, 
case when $19 = True then 1 else 0 end, $20, 
case when $21 = True then 1 else 0 end, 
case when $22 = True then 1 else 0 end, 
case when $23 = True then 1 else 0 end, 
case when $24 = True then 1 else 0 end, $25, 
case when $26 = True then 1 else 0 end, 
case when $27 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RIPTPPaymentPlanConfig/RIPTPPaymentPlanConfig_Backfill.csv';

COPY INTO STG.VC_RBCEFUNDRESPONSECODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RbcEFundResponseCode/RbcEFundResponseCode_Backfill.csv';

COPY INTO STG.VC_US_STATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
case when $1 = '' then null else $1 end, $2, 
case when $3 = True then 1 else 0 end, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/US_States/US_States_Backfill.csv';

COPY INTO STG.VC_SKIPTRACESTEP_LOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), 
case when $2 = '' then null else $2 end, 
case when $3 = '' then null else $3 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SkipTraceStep_Location/SkipTraceStep_Location_Backfill.csv';

COPY INTO STG.VC_COMMUNICATIONGROUPCHANNELVISIBILITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationGroupChannelVisibility/CommunicationGroupChannelVisibility_Backfill.csv';

COPY INTO STG.VC_DIALERJOBLOANPRODUCTENABLENEWLOAN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DialerJobLoanProductEnableNewLoan/DialerJobLoanProductEnableNewLoan_Backfill.csv';

COPY INTO STG.VC_DIALERJOB_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, 
case when $7 = True then 1 else 0 end, $8, $9, $10, $11, $12, $13, $14, $15, 
case when $16 = True then 1 else 0 end, 
case when $17 = True then 1 else 0 end, 
case when $18 = True then 1 else 0 end, 
case when $19 = True then 1 else 0 end, 
case when $20 = True then 1 else 0 end, 
case when $21 = True then 1 else 0 end, 
case when $22 = True then 1 else 0 end, 
case when $23 = True then 1 else 0 end, 
case when $24 = True then 1 else 0 end, 
case when $25 = True then 1 else 0 end, 
case when $26 = True then 1 else 0 end, 
case when $27 = True then 1 else 0 end, 
case when $28 = True then 1 else 0 end, $29, $30, $31, 
case when $32 = True then 1 else 0 end, $33, $34, $35, 
case when $36 = True then 1 else 0 end, 
case when $37 = True then 1 else 0 end, 
case when $38 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DialerJob/DialerJob_Backfill.csv';

COPY INTO STG.VC_SKIPTRACESTEP_PRODUCTCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SkipTraceStep_ProductCode/SkipTraceStep_ProductCode_Backfill.csv';

COPY INTO STG.VC_IDENTIFICATIONTYPERULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, 
case when $4 = True then 1 else 0 end, $5, $6, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IdentificationTypeRule/IdentificationTypeRule_Backfill.csv';

COPY INTO STG.VC_LOANCREDITLIMIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, $6, $7, $8, $9, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanCreditLimit/LoanCreditLimit_Backfill.csv';

COPY INTO STG.VC_WEBCALLRARRESULT2_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARResult2/WebCallRARResult2_Backfill.csv';

COPY INTO STG.VC_WEBCALLRARREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARReason/WebCallRARReason_Backfill.csv';

COPY INTO STG.VC_CARDFUNDINGSTATUSCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM'), $3, $4, $5, 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CardFundingStatusCode/CardFundingStatusCode_Backfill.csv';

COPY INTO STG.VC_TRANSCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransCode/TransCode_Backfill.csv';

COPY INTO STG.VC_RULEDEF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
case when $6 = True then 1 else 0 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8, 
to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM') end, $12
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RuleDef/RuleDef_Backfill.csv';

COPY INTO STG.VC_WEBCALLEMAILTEMPLATES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
case when $6 = True then 1 else 0 end, 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $8 = true then 1 else 0 end, 
case when $9 = True then 1 else 0 end, $10
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallEmailTemplates/WebCallEmailTemplates_Backfill.csv';

COPY INTO STG.VC_MESSAGESCENARIO_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MessageScenario/MessageScenario_Backfill.csv';

COPY INTO STG.VC_LOANDOCTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDocType/LoanDocType_Backfill.csv';

COPY INTO STG.VC_COMMUNICATIONGROUPCHANNEL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationGroupChannel/CommunicationGroupChannel_Backfill.csv';

COPY INTO STG.VC_GLACCTLOCATIONGROUPHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 
to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'), 
to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GLAcctLocationGroupHistory/GLAcctLocationGroupHistory_Backfill.csv';

COPY INTO STG.VC_FCRMAMLSERVICEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FcrmAmlServiceXRef/FcrmAmlServiceXRef_Backfill.csv';

COPY INTO STG.VC_PTPPAYMENTPLANCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = True then 1 else 0 end, 
case when $14 = True then 1 else 0 end, $15, $16, $17, $18, $19, $20, 
case when $21 = True then 1 else 0 end, 
case when $22 = True then 1 else 0 end, 
case when $23 = True then 1 else 0 end, 
case when $24 = True then 1 else 0 end, 
case when $25 = True then 1 else 0 end, 
case when $26 = True then 1 else 0 end, 
case when $27 = True then 1 else 0 end, $28, $29, $30, 
case when $31 = True then 1 else 0 end, 
case when $32 = True then 1 else 0 end, 
case when $33 = True then 1 else 0 end, $34, $35, $36, 
case when $37 = True then 1 else 0 end, 
case when $38 = True then 1 else 0 end, 
case when $39 = True then 1 else 0 end, $40, $41, 
case when contains($42, '+') then to_timestamp_ntz($42, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($42, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $43, 
to_timestamp_ntz($44, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM'), $45, $46, $47, 
case when $48 = True then 1 else 0 end, $49
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanConfig/PTPPaymentPlanConfig_Backfill.csv';

COPY INTO STG.VC_INITGLLIST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/InitGLList/InitGLList_Backfill.csv';

COPY INTO STG.VC_TELLERSECURITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerSecurity/TellerSecurity_Backfill.csv';

COPY INTO STG.VC_PHONESKILLSSEQUENCE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, 
case when $5 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PhoneSkillsSequence/PhoneSkillsSequence_Backfill.csv';

COPY INTO STG.VC_PTPPAYMENTPLANLOANPRODUCTENABLENEWLOAN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanLoanProductEnableNewLoan/PTPPaymentPlanLoanProductEnableNewLoan_Backfill.csv';

COPY INTO STG.VC_LOANPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6, 
case when $7 = True then 1 else 0 end, 
case when $8 = True then 1 else 0 end, 
case when $9 = True then 1 else 0 end, $10, $11, 
to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'), $13, 
case when $14 = True then 1 else 0 end, $15
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProduct/LoanProduct_Backfill.csv';

COPY INTO STG.VC_WEBDIALERUSER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerUser/WebDialerUser_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGTARRACTIVITYXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, case when $3 = '' then null else $3 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingTarrActivityXref/CreditReportingTarrActivityXref_Backfill.csv';

COPY INTO STG.VC_INCOMETYPELOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeTypeLocation/IncomeTypeLocation_Backfill.csv';

COPY INTO STG.VC_AUTOREPORTTAB_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, 
case when $5 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AutoReportTab/AutoReportTab_Backfill.csv';

COPY INTO STG.VC_FORMLETTERPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterProduct/FormLetterProduct_Backfill.csv';

COPY INTO STG.VC_AGENTACTION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, $6, $7, $8, 
case when $9 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AgentAction/AgentAction_Backfill.csv';

COPY INTO STG.VC_FORMLETTERLOCATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterLocation/FormLetterLocation_Backfill.csv';

COPY INTO STG.VC_AUTOREPORTEMAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AutoReportEmail/AutoReportEmail_Backfill.csv';

COPY INTO STG.VC_LOANPRODUCTCONFIGBUMPUPINCOMETYPEPRIORITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductConfigBumpUpIncomeTypePriority/LoanProductConfigBumpUpIncomeTypePriority_Backfill.csv';

COPY INTO STG.VC_WEBCALLRARRESULT1_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARResult1/WebCallRARResult1_Backfill.csv';

COPY INTO STG.VC_IATDIALERRESULTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IatDialerResults/IatDialerResults_Backfill.csv';

COPY INTO STG.VC_LOANPRODUCTTILA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 
to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM'), $19, 
case when $20 = '' then null else to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM') end, $21, $22, $23, $24, $25, $26
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductTila/LoanProductTila_Backfill.csv';

COPY INTO STG.VC_AUTOREPORTRUNSCHEDULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, 
case when $8 = True then 1 else 0 end, 
case when $9 = True then 1 else 0 end, $10, 
to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $15 = '' then null else $15 end, 
case when $16 = '' then null else $16 end, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM') end, $18
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AutoReportRunSchedule/AutoReportRunSchedule_Backfill.csv';

COPY INTO STG.VC_COUNTRY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Country/Country_Backfill.csv';

COPY INTO STG.VC_MARKETINGINVITATIONSUMMARY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, 
case when $8 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MarketingInvitationSummary/MarketingInvitationSummary_Backfill.csv';

COPY INTO STG.VC_PROCESSCONFIGDETAILHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ProcessConfigDetailHistory/ProcessConfigDetailHistory_Backfill.csv';

COPY INTO STG.VC_AGENTRESULT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AgentResult/AgentResult_Backfill.csv';

COPY INTO STG.VC_SKIPTRACESTEP_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7, $8, $9, $10, 
case when $11 = True then 1 else 0 end, 
case when $12 = True then 1 else 0 end, 
case when $13 = True then 1 else 0 end, 
case when $14 = True then 1 else 0 end, 
case when $15 = True then 1 else 0 end, 
case when $16 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SkipTraceStep/SkipTraceStep_Backfill.csv';

COPY INTO STG.VC_EXTERNALAPPCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ExternalAppConfig/ExternalAppConfig_Backfill.csv';

COPY INTO STG.VC_COLLECTIONAGINGCONFIGDAYS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionAgingConfigDays/CollectionAgingConfigDays_Backfill.csv';

COPY INTO STG.VC_ACHUSELEGACYSCHEDULEDACHCOLLECTIONSAMTLOGIC_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer)
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AchUseLegacyScheduledAchCollectionsAmtLogic/AchUseLegacyScheduledAchCollectionsAmtLogic_Backfill.csv';

COPY INTO STG.VC_MSA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MSA/MSA_Backfill.csv';

COPY INTO STG.VC_SG_RIGHTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SG_RIGHTS/SG_RIGHTS_Backfill.csv';

COPY INTO STG.VC_MESSAGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Message/Message_Backfill.csv';

COPY INTO STG.VC_DRAWERZCASH_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DrawerZCash/DrawerZCash_Backfill.csv';

COPY INTO STG.VC_EDITHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, $6, $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EditHistory/EditHistory_Backfill.csv';

COPY INTO STG.VC_BANKRUPTCYTRUSTEE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5, $6, $7, 
to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BankruptcyTrustee/BankruptcyTrustee_Backfill.csv';

COPY INTO STG.VC_CREDITCARDRESULTCODE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
case when $4 = True then 1 else 0 end, 
case when $5 = True then 1 else 0 end, 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, 
case when $8 = True then 1 else 0 end, 
case when $9 = True then 1 else 0 end, 
case when $10 = True then 1 else 0 end, 
to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, 
case when $15 = True then 1 else 0 end, $16, $17, 
case when $18 = '' then null else $18 end, 
case when $19 = True then 1 else 0 end, 
case when $20 = True then 1 else 0 end, 
case when $21 = True then 1 else 0 end, 
case when $22 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardResultCode/CreditCardResultCode_Backfill.csv';

COPY INTO STG.VC_LOANNOTE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9,
case when $10 = '' then null else $10 end, 
to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanNote/LoanNote_Backfill.csv';

COPY INTO STG.VC_TRANSDETAILINTSHORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
case when $3 = True then 1 else 0 end, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailIntShort/TransDetailIntShort_Backfill.csv';

COPY INTO STG.VC_VAULTMGRAUTHORIZATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VaultMgrAuthorization/VaultMgrAuthorization_Backfilll.csv';

COPY INTO STG.VC_SECURITYANSWER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SecurityAnswer/SecurityAnswer_Backfilll.csv';

COPY INTO STG.VC_SECURITYANSWER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SecurityAnswer/SecurityAnswer_Backfill.csv';

COPY INTO STG.VC_PROMISETOPAYCOMMUNICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PromiseToPayCommunication/PromiseToPayCommunication_Backfill.csv';

COPY INTO STG.VC_RIGHTPARTYCONTACT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM'), $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RightPartyContact/RightPartyContact_Backfill.csv';

COPY INTO STG.VC_LOANPAYMENTCHECKPAYMENTTYPEXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentCheckPaymentTypeXref/LoanPaymentCheckPaymentTypeXref_Backfill.csv';

COPY INTO STG.VC_PHYSICALCOMMUNICATIONEVENTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PhysicalCommunicationEvents/PhysicalCommunicationEvents_Backfill.csv';

COPY INTO STG.VC_WEBCALLUSERSETTING_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
cast($1 as integer), $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallUserSetting/WebCallUserSetting_Backfill.csv';

COPY INTO STG.VC_SDNLIST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SDNList/SDNList_Backfill.csv';

COPY INTO STG.VC_LOANPAYMENTDECREASEAMOUNTOWED_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentDecreaseAmountOwed/LoanPaymentDecreaseAmountOwed_Backfill.csv';

COPY INTO STG.VC_DEPOSITCHKDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DepositChkDetail/DepositChkDetail_Backfill.csv';

COPY INTO STG.VC_FORMLETTERBATCH_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, $4, 
case when $5 = True then 1 else 0 end, $6, 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterBatch/FormLetterBatch_Backfill.csv';

COPY INTO STG.VC_VAULTMGRAUTHORIZATIONDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, $9, 
case when $10 = True then 1 else 0 end, 
case when $11 = True then 1 else 0 end, $12, 
case when $13 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VaultMgrAuthorizationDetail/VaultMgrAuthorizationDetail_Backfill.csv';

COPY INTO STG.VC_ATTORNEY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = True then 1 else 0 end, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Attorney/Attorney_Backfill.csv';

COPY INTO STG.VC_FORMLETTERBATCHVENDORFILE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterBatchVendorFile/FormLetterBatchVendorFile_Backfill.csv';

COPY INTO STG.VC_PRESENTMENTREQUESTNOTSENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequestNotSent/PresentmentRequestNotSent_Backfill.csv';

COPY INTO STG.VC_CREDITVENDORDATA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, $4, $5, $6, $7, $8, $9, $10, $11, 
case when $12 = True then 1 else 0 end, 
case when $13 = True then 1 else 0 end, 
case when $14 = True then 1 else 0 end, $15, 
case when $16 = True then 1 else 0 end, 
case when $17 = '' then null else $17 end, $18, $19, 
case when $20 = True then 1 else 0 end, $21, 
case when $22 = True then 1 else 0 end, $23
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditVendorData/CreditVendorData_Backfill.csv';

COPY INTO STG.VC_DEPOSITCHK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, 
case when $6 = True then 1 else 0 end, $7, 
to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $9 = True then 1 else 0 end, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DepositChk/DepositChk_Backfill.csv';

COPY INTO STG.VC_CREDITCARDBLOCK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $4 = '' then null else $4 end, $5, $6, $7, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardBlock/CreditCardBlock_Backfill.csv';

COPY INTO STG.VC_EXCLUDEFROMCAPSHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ExcludeFromCapsHistory/ExcludeFromCapsHistory_Backfill.csv';

COPY INTO STG.VC_LOANPAYOFFDATE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM +TZH:TZM'), 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM +TZH:TZM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPayoffDate/LoanPayoffDate_Backfill.csv';

COPY INTO STG.VC_SDNMAIN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SDNMain/SDNMain_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGLOANDISPUTENOTE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM'), $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanDisputeNote/CreditReportingLoanDisputeNote_Backfill.csv';

COPY INTO STG.VC_PRESENTMENTREQUESTNOTSENTREASON_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequestNotSentReason/PresentmentRequestNotSentReason_Backfill.csv';

COPY INTO STG.VC_ZIPAREA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ZipArea/ZipArea_Backfill.csv';

COPY INTO STG.VC_MARKETINGINVITATIONHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MarketingInvitationHistory/MarketingInvitationHistory_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGLOANDISPUTE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, 
to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end, $11, 
case when $12 = '' then null else $12 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanDispute/CreditReportingLoanDispute_Backfill.csv';

COPY INTO STG.VC_LOANPRODUCTCONFIGEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductConfigEdit/LoanProductConfigEdit_Backfill.csv';

COPY INTO STG.VC_ENDOFDAYRPT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, 
case when $5 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EndOfDayRpt/EndOfDayRpt_Backfill.csv';

COPY INTO STG.VC_WEBCALLVISITORALERTSAUDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, $4, $5, $6, 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $8 = True then 1 else 0 end, $9, 
to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'), $11, 
to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallVisitorAlertsAudit/WebCallVisitorAlertsAudit_Backfill.csv';

COPY INTO STG.VC_CUSTOMERNOTE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, $4, $5, 
case when $6 = True then 1 else 0 end, 
case when $7 = True then 1 else 0 end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9,
case when $10 = '' then null else $10 end, $11, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerNote/CustomerNote_Backfill.csv';

COPY INTO STG.VC_LOANPAYMENTSUSPENDINTEREST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = True then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $5 = True then 1 else 0 end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentSuspendInterest/LoanPaymentSuspendInterest_Backfill.csv';

COPY INTO STG.VC_LOANNSFFEE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = True then 1 else 0 end, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanNSFFee/LoanNSFFee_Backfill.csv';

COPY INTO STG.VC_DOCUWAREVISITORDOCXREF_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareVisitorDocXRef/DocuwareVisitorDocXRef_Backfill.csv';

COPY INTO STG.VC_WEBCALLCENTERLOGIN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallCenterLogin/WebCallCenterLogin_Backfill.csv';

COPY INTO STG.VC_PAYMENTSPASTDUE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentsPastDue/PaymentsPastDue_Backfill.csv';

COPY INTO STG.VC_BANKACCOUNT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BankAccount/BankAccount_Backfill.csv';

COPY INTO STG.VC_CREDITCARDSEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, 
to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), $7, $8, $9
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardsEdit/CreditCardsEdit_Backfill.csv';

COPY INTO STG.VC_PROMISETOPAY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), $5, $6, $7, $8, $9, 
case when $10 = True then 1 else 0 end, 
case when $11 = True then 1 else 0 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM') end, $13, 
case when $14 = '' then null else $14 end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $16 = True then 1 else 0 end, $17, 
case when $18 = '' then null else $18 end, 
case when $19 = '' then null else $19 end, 
case when $20 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PromiseToPay/PromiseToPay_Backfill.csv';

COPY INTO STG.VC_IPBLOCK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, 
case when $6 = True then 1 else 0 end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IPBlock/IPBlock_Backfill.csv';

COPY INTO STG.VC_TELLERID_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8, $9, 
case when $10 = True then 1 else 0 end, $11, $12, 
case when $13 = True then 1 else 0 end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM') end, $16, $17, 
case when $18 = True then 1 else 0 end, $19, $20, 
case when $21 = '' then null else $21 end, 
case when $22 = '' then null else $22 end, $23, $24, $25, 
case when $26 = True then 1 else 0 end, $27, $28, 
case when $29 = '' then null else $29 end, $30, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $32 = '' then null else to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM') end, $33, $34, $35, $36
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TELLERID/TELLERID_Backfill.csv';

COPY INTO STG.VC_DIALERKEYS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, $6, 
case when $7 = True then 1 else 0 end, 
case when $8 = True then 1 else 0 end, $9, 
to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $11 = True then 1 else 0 end, $12, 
case when $13 = True then 1 else 0 end, 
case when $14 = '' then null else $14 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DialerKeys/DialerKeys_Backfill.csv';

COPY INTO STG.VC_CCARDRESPONSES_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, $6, 
case when $7 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CCardResponses/CCardResponses_Backfill.csv';

COPY INTO STG.VC_DOCUWARESTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, $7, 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareStatus/DocuwareStatus_Backfill.csv';

COPY INTO STG.VC_VAULTRECALCADJ_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, $6, $7, $8, $9, $10, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VaultRecalcAdj/VaultRecalcAdj_Backfill.csv';

COPY INTO STG.VC_PAYDAYLOAN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null else $9 end, $10, $11, 
case when $12 = True then 1 else 0 end, $13, 
case when $14 = '' then null else $14 end, 
case when $15 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaydayLoan/PaydayLoan_Backfill.csv';

COPY INTO STG.VC_LOANDOCPRINTED_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDocPrinted/LoanDocPrinted_Backfill.csv';

COPY INTO STG.VC_PAYMENTSPASTDUEDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), $4, $5, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentsPastDueDetail/PaymentsPastDueDetail_Backfill.csv';

COPY INTO STG.VC_VISITOREMAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null else $9 end, $10, $11, 
case when $12 = '' then null else $12 end, to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorEmail/VisitorEmail_Backfill.csv';

COPY INTO STG.VC_DMA_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end, $11, $12, $13, $14
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DMA/DMA_Backfill.csv';

COPY INTO STG.VC_TELLERLOGIN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $7 = True then 1 else 0 end, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerLogin/TellerLogin_Backfill.csv';

COPY INTO STG.VC_TRANSDETAILCASHPARSEDCASH_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailCashParsedCash/TransDetailCashParsedCash_Backfill.csv';

COPY INTO STG.VC_BALSHEETCOLUMNS2_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BalSheetColumns2/BalSheetColumns2_Backfill.csv';

COPY INTO STG.VC_VisitorAuthenticationCode_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, $6, $7, 
case when $8 = '' then null else $8 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorAuthenticationCode/VisitorAuthenticationCode_Backfill.csv';

COPY INTO STG.VC_CUSTOMERIDENTIFICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = True then 1 else 0 end, 
case when $11 ='' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM') end, $12, 
case when $13 = '' then null else $13 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerIdentification/CustomerIdentification_Backfill.csv';

COPY INTO STG.VC_LOANAUTHORIZEDPAYMENTMETHOD_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else $4 end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM') end, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM') end,$8, $9, 
case when $10 = True then 1 else 0 end, 
case when $11 = '' then null else $11 end, 
case when $12 = '' then null else $12 end, 
case when $13 = '' then null else $13 end, 
case when $14 = '' then null else $14 end, 
case when $15 = '' then null else $15 end, 
case when $16 = '' then null else $16 end, 
case when $17 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanAuthorizedPaymentMethod/LoanAuthorizedPaymentMethod_Backfill.csv';

COPY INTO STG.VC_IPTOCOUNTRY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IpToCountry/IpToCountry_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGPROCESSINGQUEUEHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM'), $6, $7, 
case when $8 = True then 1 else 0 end, 
to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM'), 
to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM')
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingProcessingQueueHistory/CreditReportingProcessingQueueHistory_Backfill.csv';

COPY INTO STG.VC_LOANFUNDINGHISTORYDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanFundingHistoryDetail/LoanFundingHistoryDetail_Backfill.csv';

COPY INTO STG.VC_ISSUEREDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = '' then null else $3 end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IssuerEdit/IssuerEdit_Backfill.csv';

COPY INTO STG.VC_BANK_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, $5, $6, 
case when $7 = True then 1 else 0 end, $8, $9, 
case when $10 = '' then null else $10 end, $11, $12, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM') end, $15, 
case when $16 = '' then null else to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $17 = True then 1 else 0 end, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, 
case when $30 = True then 1 else 0 end, 
case when $31 = True then 1 else 0 end, $32, $33, $34, $35, $36
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Bank/Bank_Backfill.csv';

COPY INTO STG.VC_PROMISETOPAYDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = '' then null else $10 end, $11, $12, $13, $14, 
case when $15 = '' then null else $15 end, 
case when $16 = '' then null else $16 end, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, 
case when $31 = '' then null else $31 end, 
case when $32 = '' then null else $32 end, 
case when $33 = '' then null else $33 end, 
case when $34 = '' then null else $34 end, $35, $36, $37, $38, $39, 
case when $40 = '' then null else $40 end, $41
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PromiseToPayDetail/PromiseToPayDetail_Backfill.csv';

COPY INTO STG.VC_TELLERIDEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, $6
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerIDEdit/TellerIDEdit_Backfill.csv';

COPY INTO STG.VC_VISITORDOCUMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, $7, 
case when $8 = '' then null else $8 end, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end, $11, 
case when $12 = '' then null else $12 end, 
case when $13 = '' then null else $13 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorDocument/VisitorDocument_Backfill.csv';

COPY INTO STG.VC_RISREPT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
case when $1 = '' then null else to_timestamp_ntz($1, 'MM/DD/YYYY HH12:MI:ss AM') end, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, $10, $11, $12, $13, $14, $15, $16, 
case when $17 = True then 1 else 0 end, 
case when $18 = True then 1 else 0 end, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $20 = '' then null else $20 end, 
case when $21 = '' then null else $21 end, $22, 
case when $23 = '' then null else $23 end, 
case when $24 = '' then null else $24 end, 
case when $25 = '' then null else to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $26 = '' then null else $26 end, 
case when $27 = '' then null else $27 end, $28, 
case when $29 = '' then null else $29 end, $30, $31, 
case when $32 = '' then null else $32 end, 
case when $33 = '' then null else to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $34 = '' then null else $34 end, 
case when $35 = True then 1 else 0 end, 
case when $36 = '' then null else to_timestamp_ntz($36, 'MM/DD/YYYY HH12:MI:ss AM') end, $37
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RISREPT/RISREPT_Backfill.csv';

COPY INTO STG.VC_PAYDAYLOANAPPROVAL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = True then 1 else 0 end, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaydayLoanApproval/PaydayLoanApproval_Backfill.csv';

COPY INTO STG.VC_LOANSTATUSCHANGE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = '' then null else $3 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, 
case when $7 = True then 1 else 0 end, 
case when $8 = True then 1 else 0 end, $9, 
case when $10 = True then 1 else 0 end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanStatusChange/LoanStatusChange_Backfill.csv';

COPY INTO STG.VC_CUSTOMERADDRESSEDIT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerAddressEdit/CustomerAddressEdit_Backfill.csv';

COPY INTO STG.VC_SCHEDULEDPAYMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = True then 1 else 0 end, $5
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ScheduledPayment/ScheduledPayment_Backfill.csv';

COPY INTO STG.VC_FORMLETTERPRINTED_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else $6 end, $7, 
case when $8 = '' then null else $8 end, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end, $11, 
case when $12 = '' then null else $12 end, 
case when $13 = '' then null else $13 end, 
case when $14 = '' then null else $14 end, 
case when $15 = '' then null else $15 end, $16, 
case when $17 = '' then null else $17 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterPrinted/FormLetterPrinted_Backfill.csv';

COPY INTO STG.VC_LOAN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $7 = True then 1 else 0 end, 
case when $8 = '' then null else $8 end, $9, 
case when $10 = True then 1 else 0 end, $11, $12, 
case when $13 = '' then null else $13 end, 
case when $14 = '' then null else $14 end, 
case when $15 = '' then null else $15 end, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, 
case when $29 = True then 1 else 0 end, 
case when $30 = True then 1 else 0 end, $31, 
case when $32 = '' then null else to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $33 = '' then null else to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $34 = True then 1 else 0 end, 
case when $35 = '' then null else to_timestamp_ntz($35, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $36 = True then 1 else 0 end, 
case when $37 = '' then null else to_timestamp_ntz($37, 'MM/DD/YYYY HH12:MI:ss AM') end, $38, 
case when $39 = '' then null else $39 end, 
case when $40 = '' then null else to_timestamp_ntz($40, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $41 = True then 1 else 0 end, $42, 
case when $43 = '' then null else to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $44 = '' then null else $44 end, 
case when $45 = True then 1 else 0 end, $46, $47, $48, $49, 
case when $50 = '' then null else to_timestamp_ntz($50, 'MM/DD/YYYY HH12:MI:ss AM') end, $51, 
case when $52 = '' then null else to_timestamp_ntz($52, 'MM/DD/YYYY HH12:MI:ss AM') end, $53, $54, 
case when $55 = '' then null else to_timestamp_ntz($55, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $56 = '' then null else $56 end, $57, 
case when $58 = True then 1 else 0 end, 
case when $59 = True then 1 else 0 end, 
case when $60 = True then 1 else 0 end, 
case when $61 = '' then null else to_timestamp_ntz($61, 'MM/DD/YYYY HH12:MI:ss AM') end, $62, $63, $64, $65, $66, $67, $68, $69, $70, 
case when $71 = True then 1 else 0 end, 
case when $72 = '' then null else to_timestamp_ntz($72, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $73 = '' then null else to_timestamp_ntz($73, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $74 = '' then null else to_timestamp_ntz($74, 'MM/DD/YYYY HH12:MI:ss AM') end, $75, $76, $77, $78, 
case when $79 = '' then null else to_timestamp_ntz($79, 'MM/DD/YYYY HH12:MI:ss AM') end, $80, $81, $82, $83, $84, 
case when $85 = True then 1 else 0 end, 
case when $86 = True then 1 else 0 end, 
case when $87 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Loan/Loan_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGLOANSTATUSHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when contains($2, '-') = True then to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanStatusHistory/CreditReportingLoanStatusHistory_Backfill.csv';

COPY INTO STG.VC_MPAYRECALCLOANPAYMENTADJ_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = True then 1 else 0 end, 
case when $9 = True then 1 else 0 end, 
case when $10 = True then 1 else 0 end, 
case when $11 = True then 1 else 0 end, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, 
case when $36 = '' then null else to_timestamp_ntz($36, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $37 = '' then null else to_timestamp_ntz($37, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $38 = '' then null else to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $39 = '' then null else to_timestamp_ntz($39, 'MM/DD/YYYY HH12:MI:ss AM') end, $40, $41, $42, $43, $44, $45, $46, $47
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayRecalcLoanPaymentAdj/MPayRecalcLoanPaymentAdj_Backfill.csv';

COPY INTO STG.VC_WEBCALLWORKITEMCATEGORYHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallWorkItemCategoryHistory/WebCallWorkItemCategoryHistory_Backfill.csv';

COPY INTO STG.VC_LOANAPPLICATIONIDENTIFICATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8, $9, 
case when $10 = '' then null else $10 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationIdentification/LoanApplicationIdentification_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGBASESEGMENTHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when contains($3, '+') = True then to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $4, $5, $6, $7, 
case when $8 = True then 1 else 0 end, 
case when $9 = '' then null else $9 end, $10, $11, $12, $13, $14, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $32 = '' then null else to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $33 = '' then null else to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM') end, $34, $35, $36, $37, $38, $39, 
case when $40 = '' then null else to_timestamp_ntz($40, 'MM/DD/YYYY HH12:MI:ss AM') end, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, 
case when $52 = '' then null else to_timestamp_ntz($52, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $53 = '' then null else to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingBaseSegmentHistory/CreditReportingBaseSegmentHistory_Backfill.csv';

COPY INTO STG.VC_CUSTOMERADDRESS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
coalesce($7, 'N/A'), $8, $9, $10, $11, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM') end, $16, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM') end, $18, $19, $20, $21, $22, $23, $24, $25, 
case when $26 = '' then null else to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM') end, $27, $28, $29, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM') end, $32, $33, $34, $35, 
case when $36  = '' then null else $36 end, $37, $38, $39, $40, $41, $42
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerAddress/CustomerAddress_Backfill.csv';

COPY INTO STG.VC_VISITORDEVICE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = True then 1 else 0 end, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end, $11
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorDevice/VisitorDevice_Backfill.csv';

COPY INTO STG.VC_CREDITCARDATTEMPTS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = True then 1 else 0 end, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM') end, $4, 
case when $5 = True then 1 else 0 end, $6, $7, $8
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardAttempts/CreditCardAttempts_Backfill.csv';

COPY INTO STG.VC_CUSTOMER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $5 = '' then null else $5 end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, 
case when $31 = True then 1 else 0 end, $32, 
case when $33 = '' then null else to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM') end, $34, $35, $36, $37, $38, $39, 
case when $40 = True then 1 else 0 end, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, 
case when $51 = True then 1 else 0 end, 
case when $52 = '' then null else $52 end, $53, $54, $55, $56, 
case when $57 = '' then null else to_timestamp_ntz($57, 'MM/DD/YYYY HH12:MI:ss AM') end, $58, 
case when $59 = '' then null else to_timestamp_ntz($59, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $60 = '' then null else to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM') end, $61, $62, $63, 
case when $64 = '' then null else to_timestamp_ntz($64, 'MM/DD/YYYY HH12:MI:ss AM') end, $65, 
case when $66 = '' then null else to_timestamp_ntz($66, 'MM/DD/YYYY HH12:MI:ss AM') end, $67, 
case when $68 = '' then null else to_timestamp_ntz($68, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $69 = '' then null else to_timestamp_ntz($69, 'MM/DD/YYYY HH12:MI:ss AM') end, $70, 
case when $71 = True then 1 else 0 end, $72, 
case when $73 = '' then null else $73 end, $74, 
case when $75 = '' then null else to_timestamp_ntz($75, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $76 = '' then null else $76 end, $77, $78, 
case when $79 = True then 1 else 0 end, 
case when $80 = True then 1 else 0 end, $81, $82, $83, 
case when $84 = '' then null else to_timestamp_ntz($84, 'MM/DD/YYYY HH12:MI:ss AM') end, $85, $86, $87, $88, 
case when $89 = '' then null else $89 end, $90, $91, $92, $93, $94, $95, $96, $97, $98, $99, $100, $101, 
case when $102 = '' then null else $102 end, 
case when $103 = True then 1 else 0 end, 
case when $104 = True then 1 else 0 end, $105, $106, $107, $108, $109, 
case when $110 = True then 1 else 0 end, $111, $112, 
case when $113 = '' then null else to_timestamp_ntz($113, 'MM/DD/YYYY HH12:MI:ss AM') end, $114, $115, $116, $117, $118
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Customer/Customer_Backfill.csv';

COPY INTO STG.VC_LOANAPPLICATIONPRODUCT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, 
case when $8 = '' then null else $8 end, 
case when $9 = '' then null else $9 end, 
case when $10 = True then 1 else 0 end, 
case when $11 = '' then null else $11 end, 
case when $12 = True then 1 else 0 end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $15 = '' then null else $15 end, 
case when $16 = '' then null else $16 end, 
case when $17 = '' then null else $17 end, 
case when $18 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationProduct/LoanApplicationProduct_Backfill.csv';

COPY INTO STG.VC_LOANAPPLICATIONINCOME_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM') end, $4, $5, $6, $7, $8, 
case when $9 = '' then null else $9 end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationIncome/LoanApplicationIncome_Backfill.csv';

COPY INTO STG.VC_AACBEXPORTDATAARCHIVE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
case when $21 = '' then null else $21 end, $22, $23, $24, $25, $26, $27, $28, $29, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM') end, $31
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AACbExportDataArchive/AACbExportDataArchive_Backfill_2020.csv';

COPY INTO STG.VC_AACBEXPORTDATAARCHIVE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
case when $21 = '' then null else $21 end, $22, $23, $24, $25, $26, $27, $28, $29, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM') end, $31
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AACbExportDataArchive/AACbExportDataArchive_Backfill_2021.csv';

COPY INTO STG.VC_AACBEXPORTDATAARCHIVE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
case when $21 = '' then null else $21 end, $22, $23, $24, $25, $26, $27, $28, $29, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM') end, $31
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AACbExportDataArchive/AACbExportDataArchive_Backfill_2022.csv';

COPY INTO STG.VC_ACH_HISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, $10, 
case when $11 = '' then null else $11 end, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16, 
case when $17 = '' then null else $17 end, 
case when $18 = '' then null else $18 end, 
case when $19 = True then 1 else 0 end, 
case when $20 = True then 1 else 0 end, 
case when $21 = '' then null else $21 end, 
case when $22 = '' then null else $22 end, $23, $24, $25, $26, $27, $28, 
case when $29 = True then 1 else 0 end, 
case when $30 = True then 1 else 0 end, $31, 
case when $32 = '' then null else $32 end, $33, 
case when $34 = '' then null else $34 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_History/ACH_History_Backfill_2020.csv';

COPY INTO STG.VC_ACH_HISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, $10, 
case when $11 = '' then null else $11 end, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16, 
case when $17 = '' then null else $17 end, 
case when $18 = '' then null else $18 end, 
case when $19 = True then 1 else 0 end, 
case when $20 = True then 1 else 0 end, 
case when $21 = '' then null else $21 end, 
case when $22 = '' then null else $22 end, $23, $24, $25, $26, $27, $28, 
case when $29 = True then 1 else 0 end, 
case when $30 = True then 1 else 0 end, $31, 
case when $32 = '' then null else $32 end, $33, 
case when $34 = '' then null else $34 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_History/ACH_History_Backfill_2021.csv';

COPY INTO STG.VC_ACH_HISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, $10, 
case when $11 = '' then null else $11 end, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16, 
case when $17 = '' then null else $17 end, 
case when $18 = '' then null else $18 end, 
case when $19 = True then 1 else 0 end, 
case when $20 = True then 1 else 0 end, 
case when $21 = '' then null else $21 end, 
case when $22 = '' then null else $22 end, $23, $24, $25, $26, $27, $28, 
case when $29 = True then 1 else 0 end, 
case when $30 = True then 1 else 0 end, $31, 
case when $32 = '' then null else $32 end, $33, 
case when $34 = '' then null else $34 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_History/ACH_History_Backfill_2022.csv';

COPY INTO STG.VC_ACH_HISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, $10, 
case when $11 = '' then null else $11 end, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16, 
case when $17 = '' then null else $17 end, 
case when $18 = '' then null else $18 end, 
case when $19 = True then 1 else 0 end, 
case when $20 = True then 1 else 0 end, 
case when $21 = '' then null else $21 end, 
case when $22 = '' then null else $22 end, $23, $24, $25, $26, $27, $28, 
case when $29 = True then 1 else 0 end, 
case when $30 = True then 1 else 0 end, $31, 
case when $32 = '' then null else $32 end, $33, 
case when $34 = '' then null else $34 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_History/ACH_History_Backfill_2023.csv';

COPY INTO STG.VC_COLLECTIONMOVEMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionMovement/CollectionMovement_Backfill_2020.csv';

COPY INTO STG.VC_COLLECTIONMOVEMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionMovement/CollectionMovement_Backfill_2021.csv';

COPY INTO STG.VC_COLLECTIONMOVEMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionMovement/CollectionMovement_Backfill_2022.csv';

COPY INTO STG.VC_COLLECTIONMOVEMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else $6 end, 
case when $7 = '' then null else $7 end, $8, 
case when $9 = '' then null else $9 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionMovement/CollectionMovement_Backfill_2023.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2020.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2021.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2022.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2023.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2024.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2025.csv';

COPY INTO STG.VC_MPAYAMORT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM') end, $5, $6, $7, $8, $9, $10, $11, $12, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM') end, $14, $15, $16
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayAmort/MPayAmort_Backfill_2026.csv';

COPY INTO STG.VC_PRESENTMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else $2 end, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $11 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Presentment/Presentment_Backfill_2020.csv';

COPY INTO STG.VC_PRESENTMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else $2 end, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $11 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Presentment/Presentment_Backfill_2021.csv';

COPY INTO STG.VC_PRESENTMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else $2 end, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $11 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Presentment/Presentment_Backfill_2022.csv';

COPY INTO STG.VC_PRESENTMENT_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else $2 end, $3, $4, 
case when $5 = '' then null else $5 end, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, $9, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $11 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Presentment/Presentment_Backfill_2023.csv';

COPY INTO STG.VC_PRESENTMENTREQUEST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequest/PresentmentRequest_Backfill_2020.csv';

COPY INTO STG.VC_PRESENTMENTREQUEST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequest/PresentmentRequest_Backfill_2021.csv';

COPY INTO STG.VC_PRESENTMENTREQUEST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequest/PresentmentRequest_Backfill_2022.csv';

COPY INTO STG.VC_PRESENTMENTREQUEST_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, 
case when contains($4, '+') = True then to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, $5, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequest/PresentmentRequest_Backfill_2023.csv';

COPY INTO STG.VC_LOANAPPLICATIONEMPLOYER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else $7 end, 
case when $8 = True then 1 else 0 end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, $10, $11, $12, $13, $14, $15, $16, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM') end, $18, 
case when $19 = True then 1 else 0 end, 
case when $20 = '' then null else $20 end, $21, 
case when $22 = '' then null else $22 end, 
case when $23 = True then 1 else 0 end, 
case when $24 = '' then null else to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM') end, $25, 
case when $26 = '' then null else to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $27 = '' then null else $27 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationEmployer/LoanApplicationEmployer_Backfill_2020.csv';

COPY INTO STG.VC_LOANAPPLICATIONEMPLOYER_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, 
case when $7 = '' then null else $7 end, 
case when $8 = True then 1 else 0 end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, $10, $11, $12, $13, $14, $15, $16, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM') end, $18, 
case when $19 = True then 1 else 0 end, 
case when $20 = '' then null else $20 end, $21, 
case when $22 = '' then null else $22 end, 
case when $23 = True then 1 else 0 end, 
case when $24 = '' then null else to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM') end, $25, 
case when $26 = '' then null else to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $27 = '' then null else $27 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationEmployer/LoanApplicationEmployer_Backfill_2021.csv';

COPY INTO STG.VC_VISITOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = True then 1 else 0 end, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $13 = '' then null else $13 end, $14, $15, 
case when $16 = '' then null else to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $17 = True then 1 else 0 end, 
case when $18 = True then 1 else 0 end, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM') end, $20, 
case when $21 = True then 1 else 0 end, $22, $23, $24, 
case when $25 = '' then null else $25 end, $26, $27, $28, 
case when $29 = '' then null else to_timestamp_ntz($29, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM') end, $32, 
case when $33 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Visitor/Visitor_Backfill_2020.csv';

COPY INTO STG.VC_VISITOR_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, 
case when $5 = True then 1 else 0 end, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $13 = '' then null else $13 end, $14, $15, 
case when $16 = '' then null else to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $17 = True then 1 else 0 end, 
case when $18 = True then 1 else 0 end, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM') end, $20, 
case when $21 = True then 1 else 0 end, $22, $23, $24, 
case when $25 = '' then null else $25 end, $26, $27, $28, 
case when $29 = '' then null else to_timestamp_ntz($29, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $30 = '' then null else to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $31 = '' then null else to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM') end, $32, 
case when $33 = True then 1 else 0 end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Visitor/Visitor_Backfill_2021.csv';

COPY INTO STG.VC_LOANPRODUCTCONFIG_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, 
case when $6 = True then 1 else 0 end, $7, 
case when $8 = True then 1 else 0 end, 
case when $9 = True then 1 else 0 end, $10, $11, $12, $13, 
case when $14 = True then 1 else 0 end, $15, $16, $17, $18, $19, $20, $21, $22, $23, 
case when $24 = True then 1 else 0 end, 
case when $25 = True then 1 else 0 end, $26, 
case when $27 = True then 1 else 0 end, $28, $29, $30, $31, $32, 
case when $33 = True then 1 else 0 end, $34, $35, $36, $37, $38, 
case when $39 = True then 1 else 0 end, $40, $41, 
case when $42 = True then 1 else 0 end, $43, 
case when $44 = True then 1 else 0 end, 
case when $45 = True then 1 else 0 end, 
case when $46 = True then 1 else 0 end, 
case when $47 = true then 1 else 0 end, 
case when $48 = True then 1 else 0 end, 
case when $49 = true then 1 else 0 end, $50, $51, 
case when $52 = True then 1 else 0 end, $53, $54, $55, 
case when $56 = True then 1 else 0 end, 
case when $57 = True then 1 else 0 end, 
case when $58 = True then 1 else 0 end, 
case when $59 = True then 1 else 0 end, 
case when $60 = True then 1 else 0 end, 
case when $61 = True then 1 else 0 end, 
case when $62 = true then 1 else 0 end, 
case when $63 = True then 1 else 0 end, 
case when $64 = true then 1 else 0 end, 
case when $65 = True then 1 else 0 end, 
case when $66 = True then 1 else 0 end, 
case when $67 = True then 1 else 0 end, 
case when $68 = True then 1 else 0 end, 
case when $69 = True then 1 else 0 end, 
case when $70 = True then 1 else 0 end, 
case when $71 = True then 1 else 0 end, 
case when $72 = True then 1 else 0 end, 
case when $73 = True then 1 else 0 end, $74, 
case when $75 = True then 1 else 0 end, $76, 
case when $77 = True then 1 else 0 end, 
case when $78 = True then 1 else 0 end, 
case when $79 = True then 1 else 0 end, $80, 
case when $81 = true then 1 else 0 end, 
case when $82 = True then 1 else 0 end, $83, 
case when $84 = True then 1 else 0 end, $85, 
case when $86 = True then 1 else 0 end, $87, $88, 
case when $89 = True then 1 else 0 end, 
case when $90 = True then 1 else 0 end, 
case when $91 = True then 1 else 0 end, 
case when $92 = True then 1 else 0 end, 
case when $93 = True then 1 else 0 end, $94, $95, 
case when $96 = True then 1 else 0 end, 
case when $97 = True then 1 else 0 end, $98, $99, 
case when $100 = True then 1 else 0 end, $101, $102, $103, $104, $105, $106, 
case when $107 = True then 1 else 0 end, 
case when $108 = True then 1 else 0 end, 
case when $109 = True then 1 else 0 end, 
case when $110 = True then 1 else 0 end, 
case when $111 = True then 1 else 0 end, $112, $113, 
case when $114 = True then 1 else 0 end, $115, $116, 
case when $117 = True then 1 else 0 end, $118, $119, $120, $121, $122, $123, $124, $125, $126, $127, $128, $129, $130, $131, $132, $133, $134, $135, 
case when $136 = True then 1 else 0 end, 
case when $137 = True then 1 else 0 end, $138, $139, 
case when $140 = True then 1 else 0 end, 
case when $141 = True then 1 else 0 end, 
case when $142 = True then 1 else 0 end, $143, $144, $145, $146, $147, $148, $149, $150, 
case when $151 = True then 1 else 0 end, $152, 
case when $153 = True then 1 else 0 end, $154, 
case when $155 = True then 1 else 0 end, $156, $157, 
case when $158 = True then 1 else 0 end, $159, 
case when $160 = True then 1 else 0 end, $161, 
case when $162 = True then 1 else 0 end, 
case when $163 = True then 1 else 0 end, 
case when $164 = True then 1 else 0 end, 
case when $165 = True then 1 else 0 end, $166, 
case when $167 = True then 1 else 0 end, $168, $169, $170, 
case when $171 = True then 1 else 0 end, $172, 
case when $173 = True then 1 else 0 end, $174, $175, $176, $177, 
case when $178 = True then 1 else 0 end, 
case when $179 = True then 1 else 0 end, 
case when $180 = True then 1 else 0 end, 
case when $181 = True then 1 else 0 end, 
case when $182 = True then 1 else 0 end, 
case when $183 = True then 1 else 0 end, $184, 
case when $185 = True then 1 else 0 end, $186, $187, $188, $189, 
case when $190 = True then 1 else 0 end, 
case when $191 = True then 1 else 0 end, 
case when $192 = True then 1 else 0 end, $193, $194, $195, $196, $197, $198, $199, 
case when $200 = True then 1 else 0 end, 
case when $201 = True then 1 else 0 end, 
case when $202 = True then 1 else 0 end, $203, $204, 
case when $205 = True then 1 else 0 end, $206, 
case when $207 = True then 1 else 0 end, $208, $209, $210, 
case when $211 = True then 1 else 0 end, $212, $213, $214, 
case when $215 = True then 1 else 0 end, 
case when $216 = True then 1 else 0 end, 
case when $217 = True then 1 else 0 end, $218, $219, $220, $221, 
case when $222 = True then 1 else 0 end, 
case when $223 = True then 1 else 0 end, 
case when $224 = True then 1 else 0 end, 
case when $225 = '' then null else to_timestamp_ntz($225, 'MM/DD/YYYY HH12:MI:ss AM') end, $226, $227, $228, $229, $230, $231, 
case when $232 = True then 1 else 0 end, 
case when $233 = True then 1 else 0 end, $234, $235, $236, 
case when $237 = True then 1 else 0 end, 
case when $238 = True then 1 else 0 end, $239, $240, 
case when $241 = True then 1 else 0 end, 
case when $242 = True then 1 else 0 end, $243, $244, 
case when $245 = True then 1 else 0 end, $246, 
case when $247 = True then 1 else 0 end, $248, $249, $250, $251, 
case when $252 = True then 1 else 0 end, $253, $254, $255, $256, $257, $258, 
case when $259 = True then 1 else 0 end, 
case when $260 = True then 1 else 0 end, $261, $262, 
case when $263 = True then 1 else 0 end, $264, $265, 
case when $266 = True then 1 else 0 end, 
case when $267 = '' then null else $267 end, $268, $269, 
case when $270 = True then 1 else 0 end, $271, 
case when $272 = True then 1 else 0 end, $273, 
case when $274 = '' then null else to_timestamp_ntz($274, 'MM/DD/YYYY HH12:MI:ss AM') end, $275, $276, 
case when $277 = '' then null else to_timestamp_ntz($277, 'MM/DD/YYYY HH12:MI:ss AM') end, $278, 
case when $279 = '' then null else to_timestamp_ntz($279, 'MM/DD/YYYY HH12:MI:ss AM') end, $280, 
case when $281 = True then 1 else 0 end, $282, 
case when $283 = True then 1 else 0 end, $284, 
case when $285 = True then 1 else 0 end, 
case when $286 = True then 1 else 0 end, 
case when $287 = True then 1 else 0 end, 
case when $288 = True then 1 else 0 end, $289, 
case when $290 = True then 1 else 0 end, $291, 
case when $292 = True then 1 else 0 end, $293, $294, 
case when $295 = True then 1 else 0 end, $296, $297, 
case when $298 = True then 1 else 0 end, 
case when $299 = True then 1 else 0 end, 
case when $300 = True then 1 else 0 end, 
case when $301 = True then 1 else 0 end, 
case when $302 = True then 1 else 0 end, 
case when $303 = '' then null else to_timestamp_ntz($303, 'MM/DD/YYYY HH12:MI:ss AM') end, $304, 
case when $305 = True then 1 else 0 end, $306, 
case when $307 = True then 1 else 0 end, $308, $309, 
case when $310 = True then 1 else 0 end, 
case when $311 = True then 1 else 0 end, $312, 
case when $313 = True then 1 else 0 end, 
case when $314 = True then 1 else 0 end, 
case when $315 = True then 1 else 0 end, $316, $317, $318, 
case when $319 = True then 1 else 0 end, 
case when $320 = True then 1 else 0 end, $321, 
case when $322 = True then 1 else 0 end, 
case when $323 = True then 1 else 0 end, $324, 
case when $325 = True then 1 else 0 end, 
case when $326 = True then 1 else 0 end, 
case when $327 = True then 1 else 0 end, 
case when $328 = True then 1 else 0 end, 
case when $329 = True then 1 else 0 end, $330, $331, 
case when $332 = True then 1 else 0 end, $333, $334, $335, $336, 
case when $337 = True then 1 else 0 end, $338, 
case when $339 = True then 1 else 0 end, $340, $341, 
case when $342 = True then 1 else 0 end, 
case when $343 = True then 1 else 0 end, $344, 
case when $345 = '' then null else $345 end, 
case when $346 = True then 1 else 0 end, 
case when $347 = True then 1 else 0 end, $348, $349, $350, $351, 
case when $352 = True then 1 else 0 end, 
case when $353 = True then 1 else 0 end, 
case when $354 = True then 1 else 0 end, 
case when $355 = True then 1 else 0 end, 
case when $356 = True then 1 else 0 end, 
case when $357 = True then 1 else 0 end, 
case when $358 = True then 1 else 0 end, $359, 
case when $360 = True then 1 else 0 end, $361, $362, $363, 
case when $364 = True then 1 else 0 end, $365, 
case when $366 = '' then null else $366 end, 
case when $367 = True then 1 else 0 end, 
case when $368 = True then 1 else 0 end, $369, 
case when $370 = True then 1 else 0 end, $371, 
case when $372 = True then 1 else 0 end, $373
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductConfig/LoanProductConfig_Backfill.csv';

COPY INTO STG.VC_MPAYLOAN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM'), $15, 
case when $16 = '' then null else to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM') end, $17, 
case when $18 = '' then null else to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM') end, $19, $20, 
case when $21 = '' then null else to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM') end, case when $22 = True then 1 else 0 end, $23, $24, 
case when $25 = True then 1 else 0 end, $26, $27, 
case when $28 = True then 1 else 0 end, $29, $30, 
case when $31 = True then 1 else 0 end, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, 
case when $49 = '' then null else to_timestamp_ntz($49, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $50 = True then 1 else 0 end, $51, 
case when $52 = '' then null else to_timestamp_ntz($52, 'MM/DD/YYYY HH12:MI:ss AM') end, $53, $54, $55, 
case when $56 = True then 1 else 0 end, 
case when $57 = True then 1 else 0 end, 
case when $58 = True then 1 else 0 end, 
case when $59 = True then 1 else 0 end, $60, 
case when $61 = True then 1 else 0 end, 
to_timestamp_ntz($62, 'MM/DD/YYYY HH12:MI:ss AM'), 
to_timestamp_ntz($63, 'MM/DD/YYYY HH12:MI:ss AM'), $64, $65, $66, $67, 
case when $68 = True then 1 else 0 end, $69, $70, $71, $72, $73, $74, 
case when $75 = True then 1 else 0 end, 
case when $76 = True then 1 else 0 end, 
case when $77 = True then 1 else 0 end, 
case when $78 = True then 1 else 0 end, $79, $80
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayLoan/MPayLoan_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGLOANSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, 
case when $2 = '' then null else to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM') end, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanStatus/CreditReportingLoanStatus_Backfill.csv';

COPY INTO STG.VC_GLOBALHISTORY_HIST FROM
(select METADATA$FILENAME, $1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM') end, $7, $8, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM') end, $11, $12, $13, $14, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM') end, $16, $17, $18, $19, $20, $21, $22, $23, 
case when $24 = True then 1 else 0 end, 
case when $25 = True then 1 else 0 end, 
case when $26 = True then 1 else 0 end, 
case when $27 = True then 1 else 0 end, 
case when $28 = True then 1 else 0 end, 
case when $29 = True then 1 else 0 end, 
case when $30 = True then 1 else 0 end, 
case when $31 = True then 1 else 0 end, $32, $33, $34, $35, $36, $37, $38, $39, $40, 
case when $41 = True then 1 else 0 end, $42, $43, $44, 
case when $45 = '' then null else to_timestamp_ntz($45, 'MM/DD/YYYY HH12:MI:ss AM') end, $46, $47, 
case when $48 = '' then null else to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:ss AM') end, $49, $50, 
case when $51 = True then 1 else 0 end, $52, $53, 
case when $54 = '' then null else to_timestamp_ntz($54, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $55 = '' then null else to_timestamp_ntz($55, 'MM/DD/YYYY HH12:MI:ss AM') end, $56, $57, $58, $59, $60, 
case when $61 = True then 1 else 0 end, $62, $63, 
case when $64 = True then 1 else 0 end, $65, 
case when $66 = True then 1 else 0 end, $67, $68, $69, $70, $71, $72, 
case when $73 = True then 1 else 0 end, $74, $75, 
case when $76 = True then 1 else 0 end, 
case when $77 = True then 1 else 0 end, $78, $79, $80, 
case when $81 = True then 1 else 0 end, 
case when $82 = True then 1 else 0 end, $83, 
case when $84 = True then 1 else 0 end, 
case when $85 = True then 1 else 0 end, $86, $87, $88, $89, 
case when $90 = True then 1 else 0 end, $91, 
case when $92 = True then 1 else 0 end, $93, 
case when $94 = True then 1 else 0 end, 
case when $95 = True then 1 else 0 end, $96, 
case when $97 = True then 1 else 0 end, $98, $99, $100, $101, 
case when $102 = '' then null else to_timestamp_ntz($102, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $103 = '' then null else to_timestamp_ntz($103, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GlobalHistory/GlobalHistory_Backfill.csv';

COPY INTO STG.VC_CREDITREPORTINGLOANACTIVITY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'), 
$1, $2, 
case when $3 = '' then null else $3 end, $4, 
to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM') end
from @ETL.INBOUND)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanActivity/CreditReportingLoanActivity_Backfill.csv*';