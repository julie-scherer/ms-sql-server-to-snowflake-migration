COPY INTO STG.SRC_CUSTOMERBUSINESS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    $5, 
    $6, 
    $7, 
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21, 
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    $23, 
    $24, 
    $25, 
    $26, 
    $27, 
    $28, 
    $29, 
    $30, 
    $31, 
    $32, 
    $33
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerBusiness/CustomerBusiness_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLRARRCONFIGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END, 
    $6, 
    $7, 
    $8, 
    $9,
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    $20,
    CASE WHEN $21 = '' THEN NULL 
    ELSE to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    $22,
    CASE WHEN $23 = '' THEN NULL 
    ELSE to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $24 = '' THEN NULL 
    ELSE to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $25
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARRConfigHistory/WebCallRARRConfigHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANBANKCARDPENDING_HOLD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanBankCardPending_Hold/LoanBankCardPending_Hold_Backfill.csv*';

COPY INTO STG.SRC_CFPB_ASSUMEDBADCARDAUTHS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4, 
    $5,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CFPB_AssumedBadCardAuths/CFPB_AssumedBadCardAuths_Backfill.csv*';

COPY INTO STG.SRC_GOLDDAILYBAGDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldDailyBagDetail/GoldDailyBagDetail_Backfill.csv*';

COPY INTO STG.SRC_AMLFILELOG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4,
    $5, 
    $6,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLFileLog/AMLFileLog_Backfill.csv*';

COPY INTO STG.SRC_POWEROFATTORNEY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    $12,
    CASE WHEN $13 = '' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $14,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $16 = '' THEN NULL 
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $17
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PowerOfAttorney/PowerOfAttorney_Backfill.csv*';

COPY INTO STG.SRC_SERVICETRANSNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ServiceTransNote/ServiceTransNote_Backfill.csv*';

COPY INTO STG.SRC_GOLDTRANSFERDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldTransferDetail/GoldTransferDetail_Backfill.csv*';

COPY INTO STG.SRC_CASHEDCHECKIMAGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CashedCheckImage/CashedCheckImage_Backfill.csv*';

COPY INTO STG.SRC_VISITORLOANPRODUCTANNUALRATEBAND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorLoanProductAnnualRateBand/VisitorLoanProductAnnualRateBand_Backfill.csv*';

COPY INTO STG.SRC_VEHICLEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    $8,
    $9,
    $10,
    $11
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VehicleHistory/VehicleHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANDOC_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDoc/LoanDoc_Backfill.csv*';

COPY INTO STG.SRC_BOOLEANQUESTIONRESPONSE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'), 
    $4, 
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BooleanQuestionResponse/BooleanQuestionResponse_Backfill.csv*';

COPY INTO STG.SRC_STORE_WINDOWS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    $3, 
    $4, 
    $5,
    $6,
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $13,
    $14,
    $15,
    $16,
    $17

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Store_Windows/Store_Windows_Backfill.csv*';

COPY INTO STG.SRC_CFPB_ASSUMEDBADAUTHS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    $7,
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CFPB_AssumedBadAuths/CFPB_AssumedBadAuths_Backfill.csv*';

COPY INTO STG.SRC_SPOUSALNOTIFICATIONEXPORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    $5

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SpousalNotificationExport/SpousalNotificationExport_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERLOANDOCUMENTSFILE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    $4, 
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterLoanDocumentsFile/FormLetterLoanDocumentsFile_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERBATCH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterBatch/FormLetterBatch_Backfill.csv*';

COPY INTO STG.SRC_TRANSFERFUNDSINTERSTORE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4, 
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransferFundsInterStore/TransferFundsInterStore_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGL1SEGMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingL1Segment/CreditReportingL1Segment_Backfill.csv*';

COPY INTO STG.SRC_SECURITYANSWER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SecurityAnswer/SecurityAnswer_Backfill.csv*';

COPY INTO STG.SRC_LOCATIONUS_ZIPCODESXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
	END,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LocationUS_ZipcodesXRef/LocationUS_ZipcodesXRef_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERCREDITRPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerCreditRpt/CustomerCreditRpt_Backfill.csv*';

COPY INTO STG.SRC_AMLOCCUPATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLOccupation/AMLOccupation_Backfill.csv*';

COPY INTO STG.SRC_VERGELOANTRANSFERELIGIBILITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VergeLoanTransferEligibility/VergeLoanTransferEligibility_Backfill.csv*';

COPY INTO STG.SRC_SCANNEDDOCUMENTOVERRIDE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ScannedDocumentOverride/ScannedDocumentOverride_Backfill.csv*';

COPY INTO STG.SRC_OPENENDSCHEDULEDPAYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndScheduledPayment/OpenEndScheduledPayment_Backfill.csv*';

COPY INTO STG.SRC__OELOANSOUTOFBALANCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_OELoansOutOfBalance/_OELoansOutOfBalance_Backfill.csv*';

COPY INTO STG.SRC_LOANSERVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4, 
    $5,
    $6,
    $7,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanService/LoanService_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLUSERSETTING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallUserSetting/WebCallUserSetting_Backfill.csv*';

COPY INTO STG.SRC_SKIPTRACEEVENTS_NOTUSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceEvents_NotUsed/SkipTraceEvents_NotUsed_Backfill.csv*';

COPY INTO STG.SRC_LIENHOLDER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4,
    $5,
    $6, 
    $7, 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $9, 
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21, 
    $22, 
    $23, 
    $24, 
    $25
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LienHolder/LienHolder_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONOUTOFWALLET_LEGACY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $7, 
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationOutOfWallet_LEGACY/LoanApplicationOutOfWallet_LEGACY_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARESCANERROR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5,
    $6, 
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareScanError/DocuwareScanError_Backfill.csv*';

COPY INTO STG.SRC_LOANCCALLCHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanCCallChange/LoanCCallChange_Backfill.csv*';

COPY INTO STG.SRC__RCC2022SRCONLY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    $12,
    $13,
    $14,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_RCC2022srconly/_RCC2022srconly_Backfill.csv*';

COPY INTO STG.SRC_MONEYORDERDAILYTOTAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyOrderDailyTotal/MoneyOrderDailyTotal_Backfill.csv*';

COPY INTO STG.SRC_AUTHORIZEDVISITORCONTACT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AuthorizedVisitorContact/AuthorizedVisitorContact_Backfill.csv*';

COPY INTO STG.SRC_IPBLOCK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IPBlock/IPBlock_Backfill.csv*';

COPY INTO STG.SRC_CAPSRUN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CapsRun/CapsRun_Backfill.csv*';

COPY INTO STG.SRC_SERVICEDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
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
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    $13,
    CASE WHEN $14 = '' THEN NULL 
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $15,
    $16,
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ServiceDetail/ServiceDetail_Backfill.csv*';

COPY INTO STG.SRC__RCC2022NONADASTRA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    $12,
    $13,
    $14,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_RCC2022NonAdAstra/_RCC2022NonAdAstra_Backfill.csv*';

COPY INTO STG.SRC_ACHPENDING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4,
    $5,
    $6,
    $7,
    $8, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    $10,
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END,
    $17,
    $18,
    $19,
    $20,
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $23 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM'),
    $25,
    $26,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    $28,
    $29
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHPending/ACHPending_Backfill.csv*';

COPY INTO STG.SRC_VISITORBLOCKHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorBlockHistory/VisitorBlockHistory_Backfill.csv*';

COPY INTO STG.SRC_GOLDDAILYBAG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6,
    $7,
    $8, 
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldDailyBag/GoldDailyBag_Backfill.csv*';

COPY INTO STG.SRC_ACHSENTPARENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACHSentParent/ACHSentParent_Backfill.csv*';

COPY INTO STG.SRC_DOCPRINT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocPrint/DocPrint_Backfill.csv*';

COPY INTO STG.SRC_COLLECTIONSETTLEMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $13 = '' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $14
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionSettlement/CollectionSettlement_Backfill.csv*';

COPY INTO STG.SRC_EXTERNALAPPCONFIGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExternalAppConfigHistory/ExternalAppConfigHistory_Backfill.csv*';

COPY INTO STG.SRC_PURCHASESERVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    $7,
    $8,
    $9, 
    $10,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PurchaseService/PurchaseService_Backfill.csv*';

COPY INTO STG.SRC_CARDGOVERNOROVERRIDEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorOverrideHistory/CardGovernorOverrideHistory_Backfill.csv*';

COPY INTO STG.SRC_PHONESKILLSCALL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    $18,
    $19
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PhoneSkillsCall/PhoneSkillsCall_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTSKIP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentSkip/LoanPaymentSkip_Backfill.csv*';

COPY INTO STG.SRC_VISITORNONMILITARYVERIFICATIONVAULTMGRAUTHORIZATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorNonMilitaryVerificationVaultMgrAuthorization/VisitorNonMilitaryVerificationVaultMgrAuthorization_Backfill.csv*';

COPY INTO STG.SRC_US_ZIPCODES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
pattern= 'inbound/SRC/Backfill/US_Zipcodes/US_Zipcodes_Backfill.csv*';

COPY INTO STG.SRC_RIGHTPARTYCONTACT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RightPartyContact/RightPartyContact_Backfill.csv*';

COPY INTO STG.SRC_COURTESYPAYOUT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    $11,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CourtesyPayout/CourtesyPayout_Backfill.csv*';

COPY INTO STG.SRC_IN459126_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    $12,
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM'),
    $14,
    $15,
    $16,
    $17,
    to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM'),
    $22
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IN459126/IN459126_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSRDFCONSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusRDFConsent/OptPlusRDFConsent_Backfill.csv*';

COPY INTO STG.SRC_AMLADDITIONALPARTY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6,
    $7,
    $8,
    $9, 
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $36 = 'True' THEN 1
    ELSE 0
    END,
    $37,
    $38,
    $39,
    $40,
    $41,
    $42,
    $43,
    $44
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AMLAdditionalParty/AMLAdditionalParty_Backfill.csv*';

COPY INTO STG.SRC_FORCEAPPROVALVALUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ForceApprovalValue/ForceApprovalValue_Backfill.csv*';

COPY INTO STG.SRC_CARDGOVERNORHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorHistory/CardGovernorHistory_Backfill.csv*';

COPY INTO STG.SRC_CARDREVIEW_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    $8,
    $9,
    $10
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardReview/CardReview_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGPROCESSINGQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingProcessingQueue/CreditReportingProcessingQueue_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTAUTHHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0  
    END,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9,
    $10
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentAuthHistory/PaymentAuthHistory_Backfill.csv*';

COPY INTO STG.SRC_INVALIDCUSTOMERADDRESS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InvalidCustomerAddress/InvalidCustomerAddress_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERSURVEY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0  
    END,
    $12,
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM'),
    $14,
    $15,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    CASE WHEN $20 = '' THEN NULL
    ELSE $20
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL
    ELSE $24
    END,
    CASE WHEN $25 = '' THEN NULL
    ELSE $25
    END,
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerSurvey/CustomerSurvey_Backfill.csv*';

COPY INTO STG.SRC_OUTOFWALLETQUIZLOANFUNDING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletQuizLoanFunding/OutOfWalletQuizLoanFunding_Backfill.csv*';

COPY INTO STG.SRC_OPENENDREBOOTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $6, 
    $7, 
    $8, 
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $10, 
    $11, 
    $12, 
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $15, 
    CASE WHEN $16 = '' THEN NULL 
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $17, 
    to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $19, 
    CASE WHEN $20 = '' THEN NULL 
    ELSE to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $21 = '' THEN NULL 
    ELSE to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $22, 
    CASE WHEN $23 = '' THEN NULL 
    ELSE to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $24, 
    $25, 
    $26, 
    $27, 
    $28, 
    CASE WHEN $29 = '' THEN NULL 
    ELSE to_timestamp_ntz($29, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $30, 
    CASE WHEN $31 = '' THEN NULL 
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $32, 
    $33, 
    $34, 
    $35, 
    $36, 
    $37, 
    CASE WHEN $38 = 'True' THEN 1
    ELSE 0  
    END, 
    CASE WHEN $39 = 'True' THEN 1
    ELSE 0  
    END, 
    CASE WHEN $40 = '' THEN NULL 
    ELSE to_timestamp_ntz($40, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $41 = '' THEN NULL 
    ELSE to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $42 = '' THEN NULL
    ELSE $42
    END, 
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END, 
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
    $55, 
    $56, 
    $57, 
    $58, 
    $59, 
    $60, 
    $61, 
    $62, 
    $63, 
    $64, 
    $65, 
    $66, 
    $67, 
    $68, 
    $69, 
    $70, 
    $71, 
    $72, 
    $73, 
    $74, 
    $75, 
    $76, 
    $77, 
    $78, 
    $79, 
    $80, 
    $81, 
    $82, 
    $83, 
    $84, 
    $85, 
    $86, 
    $87, 
    $88, 
    $89, 
    $90, 
    $91, 
    $92, 
    $93, 
    $94, 
    $95, 
    CASE WHEN $96 = 'True' THEN 1
    ELSE 0  
    END, 
    CASE WHEN $97 = 'True' THEN 1
    ELSE 0  
    END, 
    $98, 
    $99, 
    $100, 
    $101, 
    $102, 
    $103, 
    $104, 
    $105, 
    $106, 
    $107, 
    $108, 
    $109
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndRebootAdj/OpenEndRebootAdj_Backfill.csv*';

COPY INTO STG.SRC_BANKRUPTCYTRUSTEE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $5, 
    $6, 
    $7, 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankruptcyTrustee/BankruptcyTrustee_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERAPPDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    $4, 
    $5, 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerAppDate/CustomerAppDate_Backfill.csv*';

COPY INTO STG.SRC_IMAGECASHLETTERBUNDLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $5, 
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ImageCashLetterBundle/ImageCashLetterBundle_Backfill.csv*';

COPY INTO STG.SRC_IMAGECASHLETTERDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5, 
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ImageCashLetterDetail/ImageCashLetterDetail_Backfill.csv*';

COPY INTO STG.SRC_DMA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5, 
    $6,
    $7,
    $8,
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11,
    $12,
    $13,
    $14
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DMA/DMA_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5, 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusEdit/OptPlusEdit_Backfill.csv*';

COPY INTO STG.SRC_TELLERID_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5, 
    $6,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8,
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0  
    END, 
    $11, 
    $12, 
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $14 = '' THEN NULL 
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $15 = '' THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $16, 
    $17, 
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0  
    END,
    $19, 
    $20, 
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END, 
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END, 
    $23, 
    $24, 
    $25, 
    CASE WHEN $26 = 'True' THEN 1
    ELSE 0  
    END, 
    $27, 
    $28, 
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END, 
    $30, 
    CASE WHEN $31 = '' THEN NULL 
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $32 = '' THEN NULL 
    ELSE to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $33, 
    $34, 
    $35, 
    $36
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TELLERID/TELLERID_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERINCOMEBACKUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    $5, 
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerIncomeBackup/CustomerIncomeBackup_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARESTATUSXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareStatusXRef/DocuwareStatusXRef_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLRARRFEATURES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0  
    END, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0  
    END,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARRFeatures/WebCallRARRFeatures_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERLOANHISTORYFILE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0  
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterLoanHistoryFile/FormLetterLoanHistoryFile_Backfill.csv*';

COPY INTO STG.SRC__DSEXCLUDE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_DsExclude/_DsExclude_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERBATCHVENDORFILE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0  
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterBatchVendorFile/FormLetterBatchVendorFile_Backfill.csv*';

COPY INTO STG.SRC_TRANSACTIONNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9,
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $11, 
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionNote/TransactionNote_Backfill.csv*';

COPY INTO STG.SRC_IPTOCOUNTRY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IpToCountry/IpToCountry_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANACTIVITYHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $6,
    $7,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanActivityHistory/CreditReportingLoanActivityHistory_Backfill.csv*';

COPY INTO STG.SRC_WEBDAILYREPORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDailyReport/WebDailyReport_Backfill.csv*';

COPY INTO STG.SRC__DUP_CUSTOMERIDENTIFICATIONRECORDS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0  
    END,
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_Dup_CustomerIdentificationRecords/_Dup_CustomerIdentificationRecords_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTMPAYAUTH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentMPayAuth/LoanPaymentMPayAuth_Backfill.csv*';

COPY INTO STG.SRC_LOANDOCUSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END,
    $17,
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $23 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $25 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $26 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    $28,
    $29,
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $31 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $32 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $36 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $37 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $38 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $39 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDocUsed/LoanDocUsed_Backfill.csv*';

COPY INTO STG.SRC_BANK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $8,
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL 
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $15,
    CASE WHEN $16 = '' THEN NULL 
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0  
    END,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0  
    END,
    CASE WHEN $31 = 'True' THEN 1
    ELSE 0  
    END,
    $32,
    $33,
    $34,
    $35,
    $36
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Bank/Bank_Backfill.csv*';

COPY INTO STG.SRC_VISITORDEVICEAUTHENTICATIONCERTIFICATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDeviceAuthenticationCertificate/VisitorDeviceAuthenticationCertificate_Backfill.csv*';

COPY INTO STG.SRC_VISITORNONMILITARYVERIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorNonMilitaryVerification/VisitorNonMilitaryVerification_Backfill.csv*';

COPY INTO STG.SRC_MARKETINGINVITATIONHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MarketingInvitationHistory/MarketingInvitationHistory_Backfill.csv*';

COPY INTO STG.SRC__IN594077_RIFEES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_IN594077_RIFees/_IN594077_RIFees_Backfill.csv*';

COPY INTO STG.SRC_GOLDTRANSCUSTOMER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4, 
    $5,
    $6,
    $7,
    $8,
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0
    END,
    $18,
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $23 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $25 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $26 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $29 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $31 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldTransCustomer/GoldTransCustomer_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLWORKQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    $8,
    $9, 
    $10,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0  
    END,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    CASE WHEN $20 = '' THEN NULL
    ELSE $20
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL
    ELSE $24
    END,
    $25,
    CASE WHEN $26 = '' THEN NULL 
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    $29,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END, 
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END, 
    $34, 
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END, 
    CASE WHEN $36 = '' THEN NULL
    ELSE $36
    END, 
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END, 
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $39 = 'True' THEN 1
    ELSE 0  
    END, 
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END, 
    $41, 
    $42, 
    $43, 
    $44, 
    $45, 
    $46, 
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END, 
    $48, 
    CASE WHEN $49 = '' THEN NULL
    ELSE $49
    END, 
    $50, 
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END, 
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END, 
    CASE WHEN $53 = '' THEN NULL 
    ELSE to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $54, 
    CASE WHEN $55 = 'True' THEN 1
    ELSE 0  
    END, 
    to_timestamp_ntz($56, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $57 = 'True' THEN 1
    ELSE 0  
    END, 
    $58, 
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END, 
    $60
    
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallWorkQueue/WebCallWorkQueue_Backfill.csv*';

COPY INTO STG.SRC_GOLDTRANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4, 
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    $8,
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL
    ELSE $24
    END,
    $25,
    $26,
    $27,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    $29,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END, 
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END, 
    $34, 
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END, 
    CASE WHEN $36 = '' THEN NULL
    ELSE $36
    END, 
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END, 
    $38, 
    $39, 
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END, 
    $41, 
    $42, 
    $43, 
    $44, 
    $45, 
    $46, 
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END, 
    $48,
    $49,
    $50,
    $51
    
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldTrans/GoldTrans_Backfill.csv*';

COPY INTO STG.SRC_PAYSTUB_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4, 
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    $24,
    $25
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PayStub/PayStub_Backfill.csv*';

COPY INTO STG.SRC_SIGNATURELOANAPPROVAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    $12,
    $13,
    $14,
    $15
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SignatureLoanApproval/SignatureLoanApproval_Backfill.csv*';

COPY INTO STG.SRC_CALLCAMPAIGNQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $17
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignQueue/CallCampaignQueue_Backfill.csv*';

COPY INTO STG.SRC_CALLCAMPAIGNAPPOINTMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignAppointment/CallCampaignAppointment_Backfill.csv*';

COPY INTO STG.SRC_IMAGECASHLETTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM'),
    $14,
    $15,
    CASE WHEN $16 = '' THEN NULL 
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ImageCashLetter/ImageCashLetter_Backfill.csv*';

COPY INTO STG.SRC_GOLDTRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    $12,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldTransDetail/GoldTransDetail_Backfill.csv*';

COPY INTO STG.SRC_WEBDIALERAGENTSURVEYRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerAgentSurveyResult/WebDialerAgentSurveyResult_Backfill.csv*';

COPY INTO STG.SRC_WEBLEADSALE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadSale/WebLeadSale_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERSDNCERT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $8,
    $9,
    CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerSDNCert/CustomerSDNCert_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTACHQUEUEDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentAchQueueDetail/LoanPaymentAchQueueDetail_Backfill.csv*';

COPY INTO STG.SRC_CREDITLIMITOFFERAUDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditLimitOfferAudit/CreditLimitOfferAudit_Backfill.csv*';

COPY INTO STG.SRC_REPOCASEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RepoCaseHistory/RepoCaseHistory_Backfill.csv*';

COPY INTO STG.SRC_CFPB_BADAUTHS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CFPB_BadAuths/CFPB_BadAuths_Backfill.csv*';

COPY INTO STG.SRC_EXCLUDEFROMCAPSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExcludeFromCapsHistory/ExcludeFromCapsHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANDOCUPLOAD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4,
    $5,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDocUpload/LoanDocUpload_Backfill.csv*';

COPY INTO STG.SRC_DOCUMENTSPLITHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocumentSplitHistory/DocumentSplitHistory_Backfill.csv*';

COPY INTO STG.SRC_EXCHANGERATEDAILY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExchangeRateDaily/ExchangeRateDaily_Backfill.csv*';

COPY INTO STG.SRC_PRESENTMENTREQUESTNOTSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentRequestNotSent/PresentmentRequestNotSent_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLANREQUESTPTPXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanRequestPTPXRef/PaymentPlanRequestPTPXRef_Backfill.csv*';

COPY INTO STG.SRC_CFPB_BADLOANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CFPB_BadLoans/CFPB_BadLoans_Backfill.csv*';

COPY INTO STG.SRC_DRAWERZDRAWERBAG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerZDrawerBag/DrawerZDrawerBag_Backfill.csv*';

COPY INTO STG.SRC__COLLECTIONSDEBTRECALL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CollectionsDebtRecall/_CollectionsDebtRecall_Backfill.csv*';

COPY INTO STG.SRC_ACHUSELEGACYSCHEDULEDACHCOLLECTIONSAMTLOGIC_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
    $1
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AchUseLegacyScheduledAchCollectionsAmtLogic/AchUseLegacyScheduledAchCollectionsAmtLogic_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-11'),
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
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
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
    CASE WHEN $52 = 'True' THEN 1
    ELSE 0
    END,
    $53,
    $54,
    $55,
    $56,
    $57,
    $58,
    $59,
    $60,
    $61,
    $62,
    $63,
    $64,
    $65,
    $66,
    $67,
    $68,
    $69,
    $70,
    $71,
    $72,
    $73,
    $74,
    CASE WHEN $75 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $76 = 'True' THEN 1
    ELSE 0
    END,
    $77,
    $78,
    $79,
    $80,
    $81,
    $82,
    $83,
    $84,
    $85,
    CASE WHEN $86 = 'True' THEN 1
    ELSE 0
    END,
    $87,
    $88,
    $89,
    $90,
    $91,
    $92,
    $93,
    $94,
    $95,
    $96,
    CASE WHEN $97 = '' THEN NULL
    ELSE $97
    END,
    $98,
    $99,
    $100, 
    $101, 
    $102, 
    $103, 
    $104, 
    CASE WHEN $105 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $106 = 'True' THEN 1
    ELSE 0
    END, 
    $107, 
    $108, 
    $109, 
    $110, 
    $111, 
    $112, 
    $113, 
    $114, 
    $115, 
    $116, 
    $117, 
    $118, 
    $119, 
    $120, 
    $121, 
    $122, 
    $123, 
    $124, 
    $125, 
    $126, 
    $127, 
    $128, 
    $129, 
    $130, 
    $131, 
    $132, 
    CASE WHEN $133 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $134 = 'True' THEN 1
    ELSE 0
    END, 
    $135, 
    $136, 
    $137, 
    $138, 
    $139, 
    $140, 
    $141, 
    $142, 
    $143, 
    $144, 
    $145, 
    $146, 
    $147, 
    $148, 
    $149, 
    $150, 
    $151, 
    $152, 
    $153, 
    $154, 
    $155, 
    $156, 
    $157, 
    $158, 
    $159, 
    $160, 
    $161, 
    $162, 
    $163, 
    $164, 
    $165, 
    $166, 
    $167, 
    $168, 
    $169, 
    $170, 
    $171, 
    $172, 
    $173, 
    $174, 
    $175, 
    $176, 
    CASE WHEN $177 = 'True' THEN 1
    ELSE 0
    END, 
    $178, 
    $179, 
    $180, 
    $181, 
    $182, 
    $183, 
    $184, 
    $185, 
    $186, 
    CASE WHEN $187 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $188 = 'True' THEN 1
    ELSE 0
    END, 
    $189, 
    $190, 
    $191, 
    $192, 
    $193, 
    $194, 
    $195, 
    $196, 
    $197, 
    $198, 
    $199, 
    $200, 
    CASE WHEN $201 = 'True' THEN 1
    ELSE 0
    END, 
    $202, 
    $203, 
    $204, 
    $205, 
    $206, 
    CASE WHEN $207 = 'True' THEN 1
    ELSE 0
    END, 
    $208, 
    $209, 
    $210, 
    $211, 
    CASE WHEN $212 = 'True' THEN 1
    ELSE 0
    END, 
    $213, 
    $214, 
    $215, 
    $216, 
    $217, 
    $218, 
    $219, 
    $220, 
    CASE WHEN $221 = '' THEN NULL 
    ELSE to_timestamp_ntz($221, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $222, 
    $223, 
    $224, 
    $225, 
    $226, 
    $227, 
    $228, 
    $229, 
    $230, 
    $231, 
    $232, 
    CASE WHEN $233 = 'True' THEN 1
    ELSE 0
    END, 
    $234, 
    $235, 
    $236, 
    $237, 
    CASE WHEN $238 = 'True' THEN 1
    ELSE 0
    END, 
    $239, 
    $240, 
    $241, 
    $242, 
    CASE WHEN $243 = 'True' THEN 1
    ELSE 0
    END, 
    $244, 
    $245, 
    $246, 
    $247, 
    CASE WHEN $248 = 'True' THEN 1
    ELSE 0
    END, 
    $249, 
    $250, 
    $251, 
    $252, 
    $253, 
    $254, 
    CASE WHEN $255 = 'True' THEN 1
    ELSE 0
    END, 
    $256, 
    $257, 
    $258, 
    $259, 
    $260, 
    $261, 
    $262, 
    CASE WHEN $263 = '' THEN NULL
    ELSE $263
    END, 
    $264, 
    $265, 
    $266, 
    $267, 
    CASE WHEN $268 = 'True' THEN 1
    ELSE 0
    END, 
    $269, 
    CASE WHEN $270 = '' THEN NULL 
    ELSE to_timestamp_ntz($270, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $271, 
    $272, 
    to_timestamp_ntz($273, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $274, 
    CASE WHEN $275 = '' THEN NULL 
    ELSE to_timestamp_ntz($275, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $276, 
    CASE WHEN $277 = 'True' THEN 1
    ELSE 0
    END, 
    $278, 
    CASE WHEN $279 = 'True' THEN 1
    ELSE 0
    END, 
    $280, 
    CASE WHEN $281 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $282 = 'True' THEN 1
    ELSE 0
    END, 
    $283, 
    $284, 
    $285, 
    $286, 
    $287, 
    $288, 
    $289, 
    $290, 
    CASE WHEN $291 = 'True' THEN 1
    ELSE 0
    END, 
    $292, 
    $293, 
    CASE WHEN $294 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $295 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $296 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $297 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $298 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $299 = '' THEN NULL 
    ELSE to_timestamp_ntz($299, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $300, 
    $301, 
    $302, 
    $303, 
    $304, 
    $305, 
    CASE WHEN $306 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $307 = 'True' THEN 1
    ELSE 0
    END, 
    $308, 
    $309, 
    $310, 
    $311, 
    $312, 
    $313, 
    CASE WHEN $314 = 'True' THEN 1
    ELSE 0
    END, 
    $315, 
    $316, 
    CASE WHEN $317 = 'True' THEN 1
    ELSE 0
    END, 
    $318, 
    CASE WHEN $319 = 'True' THEN 1
    ELSE 0
    END, 
    $320, 
    CASE WHEN $321 = 'True' THEN 1
    ELSE 0
    END, 
    $322, 
    $323, 
    $324, 
    $325, 
    $326, 
    $327, 
    $328, 
    $329, 
    $330, 
    $331, 
    $332, 
    $333, 
    CASE WHEN $334 = '' THEN NULL
    ELSE $334
    END, 
    $335, 
    CASE WHEN $336 = 'True' THEN 1
    ELSE 0
    END, 
    $337, 
    $338, 
    $339, 
    CASE WHEN $340 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $341 = 'True' THEN 1
    ELSE 0
    END, 
    $342, 
    $343, 
    $344, 
    $345, 
    $346, 
    $347, 
    $348, 
    $349, 
    $350, 
    $351, 
    CASE WHEN $352 = 'True' THEN 1
    ELSE 0
    END, 
    $353, 
    $354, 
    $355, 
    $356, 
    $357, 
    $358, 
    $359, 
    $360, 
    $361, 
    $362, 
    $363, 
    $364, 
    $365, 
    $366, 
    $367, 
    $368, 
    $369, 
    $370, 
    $371, 
    $372, 
    $373, 
    $374, 
    $375, 
    $376, 
    $377, 
    $378, 
    $379, 
    $380, 
    $381, 
    $382, 
    $383, 
    $384, 
    $385, 
    $386, 
    $387, 
    $388, 
    $389, 
    $390, 
    $391, 
    $392, 
    $393, 
    $394, 
    $395, 
    $396, 
    $397, 
    $398, 
    CASE WHEN $399 = '' THEN NULL
    ELSE $399
    END, 
    CASE WHEN $400 = '' THEN NULL
    ELSE $400
    END, 
    CASE WHEN $401 = '' THEN NULL
    ELSE $401
    END, 
    $402 

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfig/LoanProductConfig_Backfill.csv*';

--comment for testing