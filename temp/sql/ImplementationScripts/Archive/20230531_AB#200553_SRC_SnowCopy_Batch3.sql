COPY INTO STG.SRC_CREDITREPORTINGPROCESSINGQUEUEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2, 
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingProcessingQueueHistory/CreditReportingProcessingQueueHistory_Backfill.csv*';

COPY INTO STG.SRC_TELLERPWDHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    $7,
    $8 
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerPwdHistory/TellerPwdHistory_Backfill.csv*';

COPY INTO STG.SRC_SKIPTRACETHREAD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2, 
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    $12,
    $13,
    $14,
    $15,
    $16,
    $17
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SkipTraceThread/SkipTraceThread_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTCHECKPAYMENTTYPEXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2, 
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentCheckPaymentTypeXref/LoanPaymentCheckPaymentTypeXref_Backfill.csv*';

COPY INTO STG.SRC_SPAYSCHEDROLLOVER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2, 
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SPaySchedRollover/SPaySchedRollover_Backfill.csv*';

COPY INTO STG.SRC_CARDBATCHSETTLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2, 
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardBatchSettle/CardBatchSettle_Backfill.csv*';

COPY INTO STG.SRC_BATCHEXECUTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    CASE WHEN $3 = '' THEN NULL
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BatchExecution/BatchExecution_Backfill.csv*';

COPY INTO STG.SRC_CREDITLIMITBUMPUPREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditLimitBumpUpReason/CreditLimitBumpUpReason_Backfill.csv*';

COPY INTO STG.SRC_RETURNCHECKDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'),
    $10,
    $11,
    $12
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ReturnCheckDetail/ReturnCheckDetail_Backfill.csv*';

COPY INTO STG.SRC__RCC2022AASPEEDY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
pattern= 'inbound/SRC/Backfill/_RCC2022aaspeedy/_RCC2022aaspeedy_Backfill.csv*';

COPY INTO STG.SRC__RCC2022ADASTRA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
pattern= 'inbound/SRC/Backfill/_RCC2022AdAstra/_RCC2022AdAstra_Backfill.csv*';

COPY INTO STG.SRC_LEGALVERIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    CASE WHEN $22 = '' THEN NULL
    ELSE to_timestamp_ntz($22, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $23,
    to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM'),
    $25
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LegalVerification/LegalVerification_Backfill.csv*';

COPY INTO STG.SRC_OUTOFWALLETQUIZ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $10,
    $11,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $14
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletQuiz/OutOfWalletQuiz_Backfill.csv*';

COPY INTO STG.SRC_DISCOUNTUSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DiscountUsed/DiscountUsed_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLVISITORALERTS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    $8,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallVisitorAlerts/WebCallVisitorAlerts_Backfill.csv*';

COPY INTO STG.SRC_AUTOREPORTEDITHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AutoReportEditHistory/AutoReportEditHistory_Backfill.csv*';

COPY INTO STG.SRC_NETSPENDTRANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7,
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0  
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/NetSpendTrans/NetSpendTrans_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGBASESEGMENTTRACE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingBaseSegmentTrace/CreditReportingBaseSegmentTrace_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANDISPUTERESPONSEDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5,
    $6,
    $7,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanDisputeResponseDetail/CreditReportingLoanDisputeResponseDetail_Backfill.csv*';

COPY INTO STG.SRC_PROMISETOPAYCOMMUNICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PromiseToPayCommunication/PromiseToPayCommunication_Backfill.csv*';

COPY INTO STG.SRC_LOANOVERRIDE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanOverride/LoanOverride_Backfill.csv*';

COPY INTO STG.SRC_DRAWERX_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerX/DrawerX_Backfill.csv*';

COPY INTO STG.SRC_LOANCREDITLIMIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanCreditLimit/LoanCreditLimit_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILINTSHORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransDetailIntShort/TransDetailIntShort_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERRESPONSE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerResponse/CustomerResponse_Backfill.csv*';

COPY INTO STG.SRC_LOANABLFACILITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanABLFacility/LoanABLFacility_Backfill.csv*';

COPY INTO STG.SRC_RBCEFUNDSECURITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RbcEFundSecurity/RbcEFundSecurity_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTSTAGEDNOTSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentStagedNotSent/LoanPaymentStagedNotSent_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARECUSTOMERIDENTIFICATIONXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareCustomerIdentificationXRef/DocuwareCustomerIdentificationXRef_Backfill.csv*';

COPY INTO STG.SRC_EOSCARDETAILDISPUTECODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EOscarDetailDisputeCode/EOscarDetailDisputeCode_Backfill.csv*';

COPY INTO STG.SRC_KBBAPICALL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3,
    $4,
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/KbbApiCall/KbbApiCall_Backfill.csv*';

COPY INTO STG.SRC_ENDOFDAYRPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EndOfDayRpt/EndOfDayRpt_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANDISPUTENOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanDisputeNote/CreditReportingLoanDisputeNote_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGBASESEGMENTHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5,
    $6, 
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0  
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    $10, 
    $11, 
    $12, 
    $13,
    $14,
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    CASE WHEN $30 = '' THEN NULL 
    ELSE to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $31 = '' THEN NULL 
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $32 = '' THEN NULL 
    ELSE to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $33 = '' THEN NULL 
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $34, 
    $35, 
    $36, 
    $37, 
    $38, 
    $39, 
    to_timestamp_ntz($40, 'MM/DD/YYYY HH12:MI:ss AM'), 
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
    to_timestamp_ntz($52, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingBaseSegmentHistory/CreditReportingBaseSegmentHistory_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSEMPLOYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    $30
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusEmployment/OptPlusEmployment_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERFEEDBACK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END, 
    $7, 
    $8, 
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'),
    $10, 
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $12, 
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFeedback/CustomerFeedback_Backfill.csv*';

COPY INTO STG.SRC_DIALERKEYS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    $9,
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    $12, 
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerKeys/DialerKeys_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANDISPUTEREQUEST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
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
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanDisputeRequest/CreditReportingLoanDisputeRequest_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANDISPUTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanDispute/CreditReportingLoanDispute_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLVISITORALERTSAUDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4,
    $5,
    $6, 
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallVisitorAlertsAudit/WebCallVisitorAlertsAudit_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTREFUND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
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
pattern= 'inbound/SRC/Backfill/LoanPaymentRefund/LoanPaymentRefund_Backfill.csv*';

COPY INTO STG.SRC_OUTOFWALLETQUESTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END, 
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $9,
    $10
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletQuestion/OutOfWalletQuestion_Backfill.csv*';

COPY INTO STG.SRC_PRESENTMENTREQUESTNOTSENTREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentRequestNotSentReason/PresentmentRequestNotSentReason_Backfill.csv*';

COPY INTO STG.SRC_DEBTSALEEXPORTDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DebtSaleExportDetail/DebtSaleExportDetail_Backfill.csv*';

COPY INTO STG.SRC_ACH_RECV_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACH_Recv/ACH_Recv_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONPRODUCTSCHEDULEDPAYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationProductScheduledPayment/LoanApplicationProductScheduledPayment_Backfill.csv*';

COPY INTO STG.SRC_WIRETRANSFERMATCHEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WireTransferMatchEdit/WireTransferMatchEdit_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERFLASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFlash/CustomerFlash_Backfill.csv*';

COPY INTO STG.SRC_DRAWERXSERVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerXService/DrawerXService_Backfill.csv*';

COPY INTO STG.SRC_CFPB_LATESTAUTHS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
pattern= 'inbound/SRC/Backfill/CFPB_LatestAuths/CFPB_LatestAuths_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONCOMMUNICATIONCONSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationCommunicationConsent/LoanApplicationCommunicationConsent_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITDEBITCARD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositDebitCard/DepositDebitCard_Backfill.csv*';

COPY INTO STG.SRC_VISITOREMAILDISPOSITION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorEmailDisposition/VisitorEmailDisposition_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANSTREAMINTERESTRATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndLoanStreamInterestRate/OpenEndLoanStreamInterestRate_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSRDFODTRANSITION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusRDFODTransition/OptPlusRDFODTransition_Backfill.csv*';

COPY INTO STG.SRC_CREDITRPTPRINT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditRptPrint/CreditRptPrint_Backfill.csv*';

COPY INTO STG.SRC_BALSHEET2_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BalSheet2/BalSheet2_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLCENTERLOGIN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallCenterLogin/WebCallCenterLogin_Backfill.csv*';

COPY INTO STG.SRC_VEHICLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    $19
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Vehicle/Vehicle_Backfill.csv*';

COPY INTO STG.SRC_OVERSHORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9, 
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    $14,
    $15,
    $16,
    $17,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OverShort/OverShort_Backfill.csv*';

COPY INTO STG.SRC_SPAYSCHEDROLLOVERDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SPaySchedRolloverDetail/SPaySchedRolloverDetail_Backfill.csv*';

COPY INTO STG.SRC_COLLECTIONNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionNote/CollectionNote_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONPENDINGREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    $10,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationPendingReason/LoanApplicationPendingReason_Backfill.csv*';

COPY INTO STG.SRC_MONEYGRAMTRANSMISSIONLOG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    $10
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyGramTransmissionLog/MoneyGramTransmissionLog_Backfill.csv*';

COPY INTO STG.SRC_DRAWERBAG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $6,
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerBag/DrawerBag_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITCHK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    $7,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    $10,
    $11
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositChk/DepositChk_Backfill.csv*';

COPY INTO STG.SRC_VAULTMGRASSIGNMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultMgrAssignment/VaultMgrAssignment_Backfill.csv*';

COPY INTO STG.SRC_MPAYAMORTDUEDATECHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MPayAmortDueDateChange/MPayAmortDueDateChange_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSDIRECTDEPOSIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusDirectDeposit/OptPlusDirectDeposit_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTDECREASEAMOUNTOWED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentDecreaseAmountOwed/LoanPaymentDecreaseAmountOwed_Backfill.csv*';

COPY INTO STG.SRC_VISITORAPIAUTHORIZATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorApiAuthorization/VisitorApiAuthorization_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANSTATUSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanStatusHistory/CreditReportingLoanStatusHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTSUSPENDINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentSuspendInterest/LoanPaymentSuspendInterest_Backfill.csv*';

COPY INTO STG.SRC_LOANFUNDINGMETHODHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFundingMethodHistory/LoanFundingMethodHistory_Backfill.csv*';

COPY INTO STG.SRC_COLLECTIONAGINGITEM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionAgingItem/CollectionAgingItem_Backfill.csv*';

COPY INTO STG.SRC__ACHHISTORYTRANSDETAILVERIFY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_achHistoryTransDetailVerify/_achHistoryTransDetailVerify_Backfill.csv*';

COPY INTO STG.SRC_GLOBALHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $7, 
    $8, 
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    $11,
    $12,
    $13,
    $14,
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    CASE WHEN $44 = '' THEN NULL
    ELSE to_timestamp_ntz($44, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $45,
    $46,
    to_timestamp_ntz($47, 'MM/DD/YYYY HH12:MI:ss AM'),
    $48,
    $49,
    $50,
    $51,
    $52,
    to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $54 = '' THEN NULL
    ELSE to_timestamp_ntz($54, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
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
    CASE WHEN $65 = 'True' THEN 1
    ELSE 0
    END,
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
    $96,
    CASE WHEN $97 = '' THEN NULL
    ELSE $97
    END,
    $98,
    $99,
    to_timestamp_ntz($100, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($101, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GlobalHistory/GlobalHistory_Backfill.csv*';

COPY INTO STG.SRC_FIRSTDATAGLOBALBINFILE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
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
    $39
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FirstDataGlobalBinFile/FirstDataGlobalBinFile_Backfill.csv*';

COPY INTO STG.SRC_OPENENDRECALCSTATEMENTSNAPSHOT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    $8, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    $14,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    $18,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    $20,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    $22,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    $24,
    CASE WHEN $25 = '' THEN NULL
    ELSE $25
    END,
    $26,
    CASE WHEN $27 = '' THEN NULL
    ELSE $27
    END,
    $28,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    $32,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    $34,
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    $38,
    CASE WHEN $39 = '' THEN NULL
    ELSE $39
    END,
    $40,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    $42,
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END,
    $44,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
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
    $52,
    CASE WHEN $53 = '' THEN NULL
    ELSE $53
    END,
    $54,
    CASE WHEN $55 = '' THEN NULL
    ELSE $55
    END,
    $56,
    CASE WHEN $57 = '' THEN NULL
    ELSE to_timestamp_ntz($57, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $58 = '' THEN NULL
    ELSE to_timestamp_ntz($58, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $59 = '' THEN NULL
    ELSE to_timestamp_ntz($59, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $60 = '' THEN NULL
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $61 = '' THEN NULL
    ELSE $61
    END,
    $62,
    CASE WHEN $63 = '' THEN NULL
    ELSE to_timestamp_ntz($63, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $64 = '' THEN NULL
    ELSE to_timestamp_ntz($64, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END,
    CASE WHEN $66 = '' THEN NULL
    ELSE $66
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE to_timestamp_ntz($67, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $68 = '' THEN NULL
    ELSE to_timestamp_ntz($68, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $69,
    $70,
    $71,
    $72,
    $73,
    $74,
    $75,
    $76,
    $77,
    $78
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndRecalcStatementSnapshot/OpenEndRecalcStatementSnapshot_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLANREQUEST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'), 
    $5,
    $6,
    $7, 
    $8, 
    $9,
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $12,
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    $17,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanRequest/PaymentPlanRequest_Backfill.csv*';

COPY INTO STG.SRC_CREDITLIMITBUMPUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM'),
    $17
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditLimitBumpUp/CreditLimitBumpUp_Backfill.csv*';

COPY INTO STG.SRC_SDNMATCH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SdnMatch/SdnMatch_Backfill.csv*';

COPY INTO STG.SRC_ACH_SENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    $3, 
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    $12,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ACH_Sent/ACH_Sent_Backfill.csv*';

COPY INTO STG.SRC_LOANNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanNote/LoanNote_Backfill.csv*';

COPY INTO STG.SRC_TRANSFERFUNDS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    $4,
    $5,
    $6,
    $7,
    $8, 
    $9,
    $10,
    $11,
    $12
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransferFunds/TransferFunds_Backfill.csv*';

COPY INTO STG.SRC_EOSCARBATCHDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    $4,
    $5,
    $6,
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
    $11,
    $12
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EOscarBatchDetail/EOscarBatchDetail_Backfill.csv*';

COPY INTO STG.SRC_DEBTSALE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $8, 
    $9,
    $10,
    $11
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DebtSale/DebtSale_Backfill.csv*';

COPY INTO STG.SRC_VEHICLEQUOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    $4,
    $5,
    $6,
    $7,
    $8, 
    $9,
    $10
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VehicleQuote/VehicleQuote_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTSPASTDUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentsPastDue/PaymentsPastDue_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLANREQUESTDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanRequestDetail/PaymentPlanRequestDetail_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERLEADNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadNote/CustomerLeadNote_Backfill.csv*';

COPY INTO STG.SRC_CHECKRETURN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CheckReturn/CheckReturn_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONSCOREHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7,
    $8, 
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationScoreHistory/LoanApplicationScoreHistory_Backfill.csv*';

COPY INTO STG.SRC_VERITECLOANID_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VeritecLoanID/VeritecLoanID_Backfill.csv*';

COPY INTO STG.SRC_RISREPTDONOTCONTACT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RisReptDoNotContact/RisReptDoNotContact_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERCREDITRPTDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerCreditRptDetail/CustomerCreditRptDetail_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERONDEMAND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterOnDemand/FormLetterOnDemand_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERTHIRDPARTYLINK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerThirdPartyLink/CustomerThirdPartyLink_Backfill.csv*';

COPY INTO STG.SRC_CREDITVENDORAPIHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditVendorApiHistory/CreditVendorApiHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTCASHADVANCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentCashAdvance/LoanPaymentCashAdvance_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERSERVICEMESSAGEDISPOSITION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $5,
    $6,
    $7,
    $8,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerServiceMessageDisposition/CustomerServiceMessageDisposition_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigEdit/LoanProductConfigEdit_Backfill.csv*';

COPY INTO STG.SRC_LOANFUNDINGACHHISTORYXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFundingAchHistoryXRef/LoanFundingAchHistoryXRef_Backfill.csv*';

COPY INTO STG.SRC_WEBLEADCALLCAMPAIGNQUEUEXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadCallCampaignQueueXRef/WebLeadCallCampaignQueueXRef_Backfill.csv*';

COPY INTO STG.SRC_OUTOFWALLETQUESTIONANSWERCHOICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletQuestionAnswerChoice/OutOfWalletQuestionAnswerChoice_Backfill.csv*';

COPY INTO STG.SRC_LOANEXPENSE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanExpense/LoanExpense_Backfill.csv*';

COPY INTO STG.SRC_PROMISETOPAYDETAILTRANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PromiseToPayDetailTrans/PromiseToPayDetailTrans_Backfill.csv*';

COPY INTO STG.SRC_COMPONENTONEREPORTLOG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3,
    $4,
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ComponentOneReportLog/ComponentOneReportLog_Backfill.csv*';

COPY INTO STG.SRC_DRAWERZCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerZCash/DrawerZCash_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $6,
    $7,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingLoanActivity/CreditReportingLoanActivity_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONVEHICLEINFORMATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7,
    $8,
    $9,
    CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $12, 
    $13,
    $14,
    $15,
    $16,
    $17, 
    $18,
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END, 
    $20, 
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END, 
    $22, 
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END, 
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
    CASE WHEN $30 = '' THEN NULL
    ELSE $30
    END, 
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END, 
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END, 
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END, 
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END, 
    $35, 
    $36, 
    $37, 
    $38, 
    CASE WHEN $39 = '' THEN NULL
    ELSE $39
    END, 
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationVehicleInformation/LoanApplicationVehicleInformation_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITBAG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
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
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
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
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositBag/DepositBag_Backfill.csv*';

COPY INTO STG.SRC_LOANCHKACCTCHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
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
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
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
    $15,
    $16,
    $17, 
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END,
    $20,
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    $23
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanChkAcctChange/LoanChkAcctChange_Backfill.csv*';

COPY INTO STG.SRC_SERVICETRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    $12, 
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    $14
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ServiceTransDetail/ServiceTransDetail_Backfill.csv*';

COPY INTO STG.SRC_REFINANCELOANAPPLICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11, 
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RefinanceLoanApplication/RefinanceLoanApplication_Backfill.csv*';

COPY INTO STG.SRC_CASHEDCHECKMICR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $12
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CashedCheckMICR/CashedCheckMICR_Backfill.csv*';

COPY INTO STG.SRC_PAYDAYLOANQUALIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaydayLoanQualification/PaydayLoanQualification_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMEREXPENSEDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerExpenseDetail/CustomerExpenseDetail_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMEREXPENSE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerExpense/CustomerExpense_Backfill.csv*';

COPY INTO STG.SRC_CREDITCARDBLOCK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    $5,
    $6,
    $7,
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardBlock/CreditCardBlock_Backfill.csv*';

COPY INTO STG.SRC_DOCUWAREVISITOREMAILATTACHMENTXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareVisitorEmailAttachmentXref/DocuwareVisitorEmailAttachmentXref_Backfill.csv*';

COPY INTO STG.SRC__ISSUERBANKACCOUNT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_IssuerBankAccount/_IssuerBankAccount_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARESTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareStatus/DocuwareStatus_Backfill.csv*';

COPY INTO STG.SRC_VISITORBLOCK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorBlock/VisitorBlock_Backfill.csv*';

COPY INTO STG.SRC_OPENENDINTERESTRATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndInterestRate/OpenEndInterestRate_Backfill.csv*';

COPY INTO STG.SRC_WEBDIALERCALLRESULTNOBLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerCallResultNoble/WebDialerCallResultNoble_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARECASHEDCHECKXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareCashedCheckXRef/DocuwareCashedCheckXRef_Backfill.csv*';

COPY INTO STG.SRC_ADASTRAWEBINVENTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AdAstraWebInventory/AdAstraWebInventory_Backfill.csv*';

COPY INTO STG.SRC_DRAWERZENTEREDPARSEDCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerZEnteredParsedCash/DrawerZEnteredParsedCash_Backfill.csv*';

COPY INTO STG.SRC_DRAWERBAGPARSEDCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerBagParsedCash/DrawerBagParsedCash_Backfill.csv*';

COPY INTO STG.SRC_BANKACCOUNT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankAccount/BankAccount_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITDEBITCARDDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositDebitCardDetail/DepositDebitCardDetail_Backfill.csv*';

COPY INTO STG.SRC_VISITORPASSWORDHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorPasswordHistory/VisitorPasswordHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANDEPOSITORDERHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7, 
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDepositOrderHistory/LoanDepositOrderHistory_Backfill.csv*';

COPY INTO STG.SRC_WEBLEADARCHIVE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadArchive/WebLeadArchive_Backfill.csv*';

COPY INTO STG.SRC_COLLECTIONSTREAM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionStream/CollectionStream_Backfill.csv*';

COPY INTO STG.SRC_DRAWERZCALCPARSEDCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerZCalcParsedCash/DrawerZCalcParsedCash_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARESCANNEDDOCXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareScannedDocXRef/DocuwareScannedDocXRef_Backfill.csv*';

COPY INTO STG.SRC__CH035718_DWDOCID_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CH035718_DWDOCID/_CH035718_DWDOCID_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERPAYMENTACCOUNT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerPaymentAccount/CustomerPaymentAccount_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerActivity/CustomerActivity_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERLASTCREDITREPORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLastCreditReport/CustomerLastCreditReport_Backfill.csv*';

COPY INTO STG.SRC_TELLERLOGINFAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'), 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerLoginFail/TellerLoginFail_Backfill.csv*';

COPY INTO STG.SRC_VAULTCOUNTSERVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultCountService/VaultCountService_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYOFFDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPayoffDate/LoanPayoffDate_Backfill.csv*';

COPY INTO STG.SRC__ACHHISTORYPRESENTMENTXREFFORCONVERT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_achHistoryPresentmentXrefForConvert/_achHistoryPresentmentXrefForConvert_Backfill.csv*';

COPY INTO STG.SRC_DRAWERZSERVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerZService/DrawerZService_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTSPASTDUEDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentsPastDueDetail/PaymentsPastDueDetail_Backfill.csv*';

COPY INTO STG.SRC_LOANNSFFEE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanNSFFee/LoanNSFFee_Backfill.csv*';

COPY INTO STG.SRC_OENDLOANINSYNCADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $4,
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $13 = '' THEN NULL
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $14, 
    CASE WHEN $15 = '' THEN NULL
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $16, 
    CASE WHEN $17 = '' THEN NULL
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $18, 
    CASE WHEN $19 = '' THEN NULL
    ELSE to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $20 = '' THEN NULL
    ELSE to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $21 = '' THEN NULL
    ELSE to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
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
    CASE WHEN $51 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $52 = '' THEN NULL
    ELSE to_timestamp_ntz($52, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $53 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $54 = '' THEN NULL
    ELSE to_timestamp_ntz($54, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $55, 
    CASE WHEN $56 = '' THEN NULL
    ELSE to_timestamp_ntz($56, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $57, 
    CASE WHEN $58 = '' THEN NULL
    ELSE to_timestamp_ntz($58, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $59, 
    CASE WHEN $60 = '' THEN NULL
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $61 = '' THEN NULL
    ELSE to_timestamp_ntz($61, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $62 = '' THEN NULL
    ELSE to_timestamp_ntz($62, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
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
    CASE WHEN $77 = 'True' THEN 1
    ELSE 0
    END, 
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
    CASE WHEN $92 = '' THEN NULL
    ELSE to_timestamp_ntz($92, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $93 = '' THEN NULL
    ELSE to_timestamp_ntz($93, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $94, 
    $95, 
    $96, 
    $97, 
    $98, 
    CASE WHEN $99 = '' THEN NULL
    ELSE to_timestamp_ntz($99, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $100 = '' THEN NULL
    ELSE to_timestamp_ntz($100, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $101, 
    $102, 
    $103, 
    $104, 
    $105, 
    $106, 
    CASE WHEN $107 = '' THEN NULL
    ELSE to_timestamp_ntz($107, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $108 = '' THEN NULL
    ELSE to_timestamp_ntz($108, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $109 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $110 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $111 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $112 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $113 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $114 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $115 = 'True' THEN 1
    ELSE 0
    END, 
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
    CASE WHEN $128 = '' THEN NULL
    ELSE $128
    END, 
    $129, 
    $130, 
    $131
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OEndLoanInSyncAdj/OEndLoanInSyncAdj_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANSTREAM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $4 = '' THEN NULL
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END, 
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
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
    CASE WHEN $37 = 'True' THEN 1
    ELSE 0
    END, 
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
    $54
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndLoanStream/OpenEndLoanStream_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGBASESEGMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'), 
    $4,
    $5,
    $6, 
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    $10, 
    $11, 
    $12, 
    $13, 
    $14, 
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM'), 
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
    to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $31 = '' THEN NULL
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $32 = '' THEN NULL
    ELSE to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $33 = '' THEN NULL
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $34, 
    $35, 
    $36, 
    $37, 
    $38, 
    $39, 
    to_timestamp_ntz($40, 'MM/DD/YYYY HH12:MI:ss AM'), 
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
    $51
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingBaseSegment/CreditReportingBaseSegment_Backfill.csv*';

COPY INTO STG.SRC_WIRETRANSFERMATCH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END, 
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    $9, 
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    $12, 
    $13, 
    to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $15,
    $16, 
    $17, 
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END, 
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END, 
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
    $52,
    $53,
    $54
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WireTransferMatch/WireTransferMatch_Backfill.csv*';

COPY INTO STG.SRC_VAULTCOUNT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6, 
    $7, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    $10,
    $11, 
    $12, 
    $13, 
    $14, 
    $15,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END, 
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
    to_timestamp_ntz($42, 'MM/DD/YYYY HH12:MI:ss AM'), 
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
    $53
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultCount/VaultCount_Backfill.csv*';

COPY INTO STG.SRC_OPENENDRECALCLOANPAYMENTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    $52,
    $53,
    $54,
    $55,
    $56,
    $57,
    $58,
    $59,
    CASE WHEN $60 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $61 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $62 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $63 = 'True' THEN 1
    ELSE 0
    END,
    $64,
    $65,
    CASE WHEN $66 = '' THEN NULL
    ELSE to_timestamp_ntz($66, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE to_timestamp_ntz($67, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $68 = '' THEN NULL
    ELSE to_timestamp_ntz($68, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $69 = '' THEN NULL
    ELSE to_timestamp_ntz($69, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
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
    $89
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndRecalcLoanPaymentAdj/OpenEndRecalcLoanPaymentAdj_Backfill.csv*';

COPY INTO STG.SRC_CFPB_AUTHORIZATIONGROUPS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $18, 
    $19, 
    $20, 
    $21, 
    $22, 
    $23, 
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0
    END, 
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
    $35
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CFPB_AuthorizationGroups/CFPB_AuthorizationGroups_Backfill.csv*';

COPY INTO STG.SRC__RISREPTCOLLECTIONSDEBTRECALL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    to_timestamp_ntz($1, 'MM/DD/YYYY HH12:MI:ss AM'),
    $2,
    $3, 
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $7, 
    $8, 
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
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
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $19 = '' THEN NULL
    ELSE to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $20 = '' THEN NULL
    ELSE $20
    END, 
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END, 
    $22, 
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END, 
    CASE WHEN $24 = '' THEN NULL
    ELSE $24
    END, 
    CASE WHEN $25 = '' THEN NULL
    ELSE to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END, 
    $27, 
    $28, 
    $29, 
    $30,
    $31, 
    $32, 
    CASE WHEN $33 = '' THEN NULL
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $34, 
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($36, 'MM/DD/YYYY HH12:MI:ss AM'),
    $37
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_RISREPTCollectionsDebtRecall/_RISREPTCollectionsDebtRecall_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSCARDDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    $8, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END, 
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END, 
    $14, 
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $17 = '' THEN NULL
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END,  
    $18, 
    $19, 
    $20, 
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END, 
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
    CASE WHEN $38 = '' THEN NULL
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusCardDetail/OptPlusCardDetail_Backfill.csv*';

COPY INTO STG.SRC_TITLELOANAPPROVAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    $12, 
    $13, 
    $14, 
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    $16, 
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0
    END,  
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
    to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $25, 
    $26, 
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END, 
    $28, 
    $29, 
    $30,
    $31, 
    $32, 
    $33,
    $34
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TitleLoanApproval/TitleLoanApproval_Backfill.csv*';

COPY INTO STG.SRC_PARSECASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    $27
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ParseCash/ParseCash_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLANREQUESTDETAILPAYMENTINFO_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
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
pattern= 'inbound/SRC/Backfill/PaymentPlanRequestDetailPaymentInfo/PaymentPlanRequestDetailPaymentInfo_Backfill.csv*';

COPY INTO STG.SRC_PROMISETOPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $13, 
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END, 
    CASE WHEN $15 = '' THEN NULL
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END, 
    $17,  
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END, 
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END, 
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PromiseToPay/PromiseToPay_Backfill.csv*';

COPY INTO STG.SRC_DRAWERZ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    CASE WHEN $15 = '' THEN NULL
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16, 
    $17,  
    $18, 
    $19
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DrawerZ/DrawerZ_Backfill.csv*';

COPY INTO STG.SRC_CARDFUNDINGTRANSACTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3, 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    $5, 
    $6, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    $12, 
    $13, 
    $14, 
    $15,
    $16
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardFundingTransaction/CardFundingTransaction_Backfill.csv*';

COPY INTO STG.SRC_COLLBONUSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
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
    $15
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollBonusDetail/CollBonusDetail_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILCARDPRODUCTS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    $13
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransDetailCardProducts/TransDetailCardProducts_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERIDENTIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
pattern= 'inbound/SRC/Backfill/CustomerIdentification/CustomerIdentification_Backfill.csv*';

COPY INTO STG.SRC_TELETRACKREPORTINGDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
    $4, 
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    $9, 
    $10,
    $11, 
    $12, 
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TeletrackReportingData/TeletrackReportingData_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3, 
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
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11, 
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerNote/CustomerNote_Backfill.csv*';

COPY INTO STG.SRC_WEBPIXELVENDORDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4, 
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11, 
    $12
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebPixelVendorData/WebPixelVendorData_Backfill.csv*';

COPY INTO STG.SRC_MOSTATUSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4, 
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $8, 
    $9, 
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MOStatusHistory/MOStatusHistory_Backfill.csv*';

COPY INTO STG.SRC_VAULTRECALCADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultRecalcAdj/VaultRecalcAdj_Backfill.csv*';

COPY INTO STG.SRC_VISITORDEVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDevice/VisitorDevice_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERIDENTIFICATIONEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerIdentificationEdit/CustomerIdentificationEdit_Backfill.csv*';

COPY INTO STG.SRC_GOODCUSTOMERSTUDY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
pattern= 'inbound/SRC/Backfill/GoodCustomerStudy/GoodCustomerStudy_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTSPASTDUEBACKFILL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4, 
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentsPastDueBackfill/PaymentsPastDueBackfill_Backfill.csv*';

COPY INTO STG.SRC_ISSUEREDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IssuerEdit/IssuerEdit_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANCYCLECUSTOMERSNAPSHOT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4, 
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndLoanCycleCustomerSnapshot/OpenEndLoanCycleCustomerSnapshot_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERFLASHRESPONSE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
    
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFlashResponse/CustomerFlashResponse_Backfill.csv*';

COPY INTO STG.SRC_SCANNEDDOCUMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ScannedDocument/ScannedDocument_Backfill.csv*';

COPY INTO STG.SRC_ACHRECVITEM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AchRecvItem/AchRecvItem_Backfill.csv*';

COPY INTO STG.SRC_COLLBONUSTASKS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollBonusTasks/CollBonusTasks_Backfill.csv*';

COPY INTO STG.SRC_TELLERIDEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerIDEdit/TellerIDEdit_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLLOGGINGCATEGORYLOG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallLoggingCategoryLog/WebCallLoggingCategoryLog_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERLEADACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4, 
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerLeadActivity/CustomerLeadActivity_Backfill.csv*';

COPY INTO STG.SRC_VISITORAUTHENTICATIONCODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorAuthenticationCode/VisitorAuthenticationCode_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTSTAGED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentStaged/LoanPaymentStaged_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONSTATUSCHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $5,
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationStatusChange/LoanApplicationStatusChange_Backfill.csv*';

COPY INTO STG.SRC_VISITORAUTHENTICATIONCODEATTEMPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorAuthenticationCodeAttempt/VisitorAuthenticationCodeAttempt_Backfill.csv*';

COPY INTO STG.SRC_VAULTCOUNTCALCPARSEDCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultCountCalcParsedCash/VaultCountCalcParsedCash_Backfill.csv*';

COPY INTO STG.SRC_VAULTCOUNTENTEREDPARSEDCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultCountEnteredParsedCash/VaultCountEnteredParsedCash_Backfill.csv*';

COPY INTO STG.SRC_PRESENTMENTREQUESTACHHISTORYXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentRequestACHHistoryXRef/PresentmentRequestACHHistoryXRef_Backfill.csv*';

COPY INTO STG.SRC_LOANFUNDINGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFundingHistory/LoanFundingHistory_Backfill.csv*';

COPY INTO STG.SRC_COLLECTIONAGINGITEMBACKFILL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionAgingItemBackfill/CollectionAgingItemBackfill_Backfill.csv*';

COPY INTO STG.SRC_PRESENTMENTCREDITCARDTRANSXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PresentmentCreditCardTransXRef/PresentmentCreditCardTransXRef_Backfill.csv*';

COPY INTO STG.SRC__ACHUNIQUEPRESENTMENT1904CONVERT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_AchUniquePresentment1904Convert/_AchUniquePresentment1904Convert_Backfill.csv*';

COPY INTO STG.SRC_DOCUWAREVISITORDOCXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocuwareVisitorDocXRef/DocuwareVisitorDocXRef_Backfill.csv*';

COPY INTO STG.SRC_COMPANYHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-31'),
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
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0 
    END, 
    to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $26, 
    $27, 
    $28, 
    $29, 
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0 
    END, 
    $31, 
    $32, 
    $33,
    $34, 
    to_timestamp_ntz($35, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $36, 
    $37, 
    $38, 
    $39, 
    $40, 
    $41, 
    $42,
    $43, 
    $44, 
    CASE WHEN $45 = 'True' THEN 1
    ELSE 0 
    END, 
    $46, 
    $47, 
    $48, 
    CASE WHEN $49 = '' THEN NULL
    ELSE $49
    END, 
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
    CASE WHEN $76 = 'True' THEN 1
    ELSE 0 
    END, 
    $77, 
    $78, 
    $79, 
    $80, 
    $81, 
    $82, 
    CASE WHEN $83 = '' THEN NULL
    ELSE $83
    END, 
    $84, 
    $85, 
    $86, 
    $87, 
    $88, 
    $89, 
    $90, 
    CASE WHEN $91 = '' THEN NULL
    ELSE $91
    END, 
    $92, 
    $93, 
    $94, 
    $95, 
    $96, 
    $97, 
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
    $109, 
    $110, 
    $111, 
    $112, 
    $113,
    $114,
    $115, 
    CASE WHEN $116 = '' THEN NULL
    ELSE to_timestamp_ntz($116, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $117, 
    CASE WHEN $118 = '' THEN NULL
    ELSE $118
    END, 
    CASE WHEN $119 = '' THEN NULL
    ELSE $119
    END, 
    CASE WHEN $120 = '' THEN NULL
    ELSE $120
    END, 
    CASE WHEN $121 = '' THEN NULL
    ELSE $121
    END, 
    CASE WHEN $122 = '' THEN NULL
    ELSE $122
    END, 
    CASE WHEN $123 = '' THEN NULL
    ELSE $123
    END, 
    $124, 
    $125, 
    $126, 
    $127, 
    $128, 
    $129, 
    $130, 
    $131,
    $132,
    $133,
    $134,
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
    CASE WHEN $154 = 'True' THEN 1
    ELSE 0 
    END, 
    $155, 
    CASE WHEN $156 = 'True' THEN 1
    ELSE 0 
    END, 
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
    to_timestamp_ntz($170, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($171, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $172, 
    $173
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyHistory/CompanyHistory_Backfill.csv*';