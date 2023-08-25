COPY INTO STG.VC_ACH_RETURNCODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7,
    $8,
    $9, 
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_ReturnCode/ACH_ReturnCode_Backfill.csv*';

COPY INTO STG.VC_CARDGOVERNORVALIDATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
     CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4,
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'),
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7,
    try_to_timestamp($8)
    
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CardGovernorValidation/CardGovernorValidation_Backfill.csv*';

COPY INTO STG.VC_COMMUNICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $7,
    try_to_timestamp($8)


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Communication/Communication_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGLOANPRODUCTLOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'),
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanProductLocation/CreditReportingLoanProductLocation_Backfill.csv*';

COPY INTO STG.VC_FORMLETTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2, 
    $3,
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM'),
    $21, 
    to_timestamp_ntz($22, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $23, 
    $24, 
    $25, 
    $26, 
    $27, 
    try_to_timestamp($28), 
    $29, 
    $30, 
    $31, 
    $32, 
    $33, 
    $34, 
    $35, 
    $36, 
    $37, 
    $38, 
    $39, 
    $40, 
    $41, 
    $42, 
    $43, 
    $44, 
    $45, 
    $46, 
    $47, 
    $48, 
    $49, 
    $50, 
    $51, 
    $52, 
    $53, 
    $54, 
    $55

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetter/FormLetter_Backfill.csv*';

COPY INTO STG.VC_LOANDOC_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3, 
    $4, 
    $5,
    $6,
    $7, 
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDoc/LoanDoc_Backfill.csv*';

COPY INTO STG.VC_LOANPAYMENTSTAGEDNOTSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentStagedNotSent/LoanPaymentStagedNotSent_Backfill.csv*';

COPY INTO STG.VC_PENDINGREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PendingReason/PendingReason_Backfill.csv*';

COPY INTO STG.VC_TELLERTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerType/TellerType_Backfill.csv*';

COPY INTO STG.VC_US_ZIPCODES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/US_Zipcodes/US_Zipcodes_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    CASE WHEN $8 ='True' THEN 1
    ELSE 0
    END, 
    $9, 
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 ='True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    $17,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARR/WebCallRARR_Backfill.csv*';

COPY INTO STG.VC_WEBPIXELVENDOR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-13'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6, 
    $7, 
    $8, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    $10, 
    $11,
    $12,
    $13

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebPixelVendor/WebPixelVendor_Backfill.csv*';

--DELETE BAD DATA FROM VC_WEBPIXELVENDORREASONPULLED_HIST Table
DELETE FROM STG.VC_WEBPIXELVENDORREASONPULLED_HIST WHERE FILENAME = 'inbound/VC/Backfill/TellerType/TellerType_Backfill.csv';

