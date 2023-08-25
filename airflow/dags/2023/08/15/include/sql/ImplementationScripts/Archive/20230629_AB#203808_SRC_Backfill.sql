COPY INTO STG.SRC_AMLTHRESHOLDRULE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
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
case when $16 = 'True' then 1 else 0 end, 
case when $17 = '' then null else to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:SS AM') end, $18, 
case when $19 = '' then null else to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:SS AM') end, $20, 
case when $21 = 'True' then 1 else 0 end, 
case when $22 = 'True' then 1 else 0 end
from @ETL.inbound/SRC/Backfill/AMLThresholdRule/)
FILE_FORMAT = (format_name = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLThresholdRule/AMLThresholdRule_Backfill.csv';

COPY INTO STG.SRC_CARDGOVERNORACTIONTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, $3
from @ETL.inbound/SRC/Backfill/CardGovernorActionType/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorActionType/CardGovernorActionType_Backfill.csv';

COPY INTO STG.SRC_CARDGOVERNORVALIDATION_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $6 = 'True' then 1 else 0 end, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.inbound/SRC/Backfill/CardGovernorValidation/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorValidation/CardGovernorValidation_Backfill.csv';

COPY INTO STG.SRC_COMMUNICATIONCONSENTCONFIGHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, $3, $4, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $7 = '' then null else $7 end, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.inbound/SRC/Backfill/CommunicationConsentConfigHistory/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CommunicationConsentConfigHistory/CommunicationConsentConfigHistory_Backfill.csv';

COPY INTO STG.SRC_COMPANYDETAILHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, $3, $4, $5, $6, $7, $8, $9, 
case when $10 = '' then null else to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.inbound/SRC/Backfill/CompanyDetailHistory/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyDetailHistory/CompanyDetailHistory_Backfill.csv';

COPY INTO STG.SRC_CREDITREPORTINGSTATUS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end
from @ETL.inbound/SRC/Backfill/CreditReportingStatus/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingStatus/CreditReportingStatus_Backfill.csv';

COPY INTO STG.SRC_GALILEOALERTTYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, 
case when $3 = '' then null else to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM') end, $4, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = 'True' then 1 else 0 end
from @ETL.inbound/SRC/Backfill/GalileoAlertType/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GalileoAlertType/GalileoAlertType_Backfill.csv';

COPY INTO STG.SRC_INSURANCESTATUSHISTORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = 'True' then 1 else 0 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $13 = '' then null else to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.inbound/SRC/Backfill/InsuranceStatusHistory/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InsuranceStatusHistory/InsuranceStatusHistory_Backfill.csv';

COPY INTO STG.SRC_PREPAIDCARDBIN_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, $3, $4, 
case when $5 = '' then null else to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:SS AM') end, $6, 
case when $7 = '' then null else to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:SS AM') end, $8, $9, $10, $11, 
case when $12 = '' then null else $12 end, $13, $14, $15
from @ETL.inbound/SRC/Backfill/PrepaidCardBin/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PrepaidCardBin/PrepaidCardBin_Backfill.csv';

COPY INTO STG.SRC_LOANPRODUCTFEATURETYPE_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, 
case when $3 = 'True' then 1 else 0 end, 
case when $4 = '' then null else to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:SS AM') end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7
from @ETL.inbound/SRC/Backfill/LoanProductFeatureType/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductFeatureType/LoanProductFeatureType_Backfill.csv';

COPY INTO STG.SRC_WEBCALLCATEGORY_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-29'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, 
case when $5 = 'True' then 1 else 0 end, 
case when $6 = '' then null else $6 end, $7, 
case when $8 = 'True' then 1 else 0 end, 
case when $9 = 'True' then 1 else 0 end, 
case when $10 = 'True' then 1 else 0 end, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when contains($12, '+') = True then to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM +TZH:TZM') else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM') end, 
case when $13 = '' then null end, $14, $15
from @ETL.INBOUND/SRC/Backfill/WebCallCategory/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCategory/WebCallCategory_Backfill.csv';