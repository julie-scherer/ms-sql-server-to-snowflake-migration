COPY INTO STG.SRC_WEBDIALERUSER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND/SRC/Backfill/WebDialerUser/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*WebDialerUser_Backfill.csv*';


COPY INTO STG.SRC_CREDITREPORTINGLOANSTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END
  
FROM @ETL.INBOUND/SRC/Backfill/CreditReportingLoanStatus/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*CreditReportingLoanStatus_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGLOANACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $6,
    $7
  
FROM @ETL.INBOUND/SRC/Backfill/CreditReportingLoanActivity/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*CreditReportingLoanActivity_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTDUEDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    to_timestamp_ntz($3, ' HH12:MI:ss AM'),
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND/SRC/Backfill/LoanPaymentDueDate/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*LoanPaymentDueDate_Backfill.csv*';

COPY INTO STG.SRC_TELETRACKREPORTINGDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
  
FROM @ETL.INBOUND/SRC/Backfill/TeletrackReportingData/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*TeletrackReportingData_Backfill.csv*';

COPY INTO STG.SRC_TELLERLOGIN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $8
  
FROM @ETL.INBOUND/SRC/Backfill/TellerLogin/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*TellerLogin_Backfill.csv*';

COPY INTO STG.SRC_WEBLEADPOSTDATAVALUES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND/SRC/Backfill/WebLeadPostDataValues/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*WebLeadPostDataValues.csv*';

COPY INTO STG.SRC_OPENENDLOANSTREAMSTATEMENTSNAPSHOT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-21'),
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
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    $8, 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
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
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0
    END,
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
    $49
  
FROM @ETL.INBOUND/SRC/Backfill/OpenEndLoanStreamStatementSnapshot/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*OpenEndLoanStreamStatementSnapshot_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITCHKDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END
 
FROM @ETL.INBOUND/SRC/Backfill/DepositChkDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*DepositChkDetail_Backfill.csv*';

COPY INTO STG.SRC_SCHEDULEDPAYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4,
    $5
 
FROM @ETL.INBOUND/SRC/Backfill/ScheduledPayment/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*ScheduledPayment_Backfill.csv*';

COPY INTO STG.SRC_PRESENTMENTREQUEST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM'),
    $5,
    $6,
    $7,
    $8,
    $9,
    $10
 
FROM @ETL.INBOUND/SRC/Backfill/PresentmentRequest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*PresentmentRequest_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILCHECK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10
 
FROM @ETL.INBOUND/SRC/Backfill/TransDetailCheck/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailCheck_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSRDFAUTHORIZEDTRANSACTIONS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM'),
    $19
 
FROM @ETL.INBOUND/SRC/Backfill/OptPlusRDFAuthorizedTransactions/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*OptPlusRDFAuthorizedTransactions_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITBAGDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    to_timestamp_ntz($9, 'YYYY-MM-DD HH24:MI:SS'), 
    $10,
    $11,
    to_timestamp_ntz($12, 'YYYY-MM-DD HH24:MI:SS'),
    $13
 
FROM @ETL.INBOUND/SRC/Backfill/DepositBagDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*DepositBagDetail_Backfill.csv*';

COPY INTO STG.SRC_PAYDAYLOANAPPROVAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    $29
 
FROM @ETL.INBOUND/SRC/Backfill/PaydayLoanApproval/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*PaydayLoanApproval_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    to_timestamp_ntz($3, 'YYYY-MM-DD HH24:MI:SS'),
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    to_date($10)
 
FROM @ETL.INBOUND/SRC/Backfill/LoanApplicationIncome/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanApplicationIncome_Backfill.csv*';

COPY INTO STG.SRC_LOANDOCPRINTED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'YYYY-MM-DD HH24:MI:SS'),
    $8
 
FROM @ETL.INBOUND/SRC/Backfill/LoanDocPrinted/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanDocPrinted_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANSTATEMENTSNAPSHOT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    to_date($28),
    to_date($29),
    $30,
    to_date($31),
    $32,
    to_date($33),    
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
    $50
 
FROM @ETL.INBOUND/SRC/Backfill/OpenEndLoanStatementSnapshot/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*OpenEndLoanStatementSnapshot_Backfill.csv*';

COPY INTO STG.SRC_SPAYLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    $32
  
FROM @ETL.INBOUND/SRC/Backfill/SPayLoan/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*SPayLoan_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTOPENENDSTREAM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    $48
  
FROM @ETL.INBOUND/SRC/Backfill/LoanPaymentOpenEndStream/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanPaymentOpenEndStream_Backfill.csv*';

COPY INTO STG.SRC_ACH_HISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'YYYY-MM-DD HH24:MI:SS'),
    to_timestamp_ntz($8, 'YYYY-MM-DD HH24:MI:SS'),
    $9,
    $10,
    $11,
    $12,
    to_timestamp_ntz($13, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
    to_date($33),
    $34
  
FROM @ETL.INBOUND/SRC/Backfill/ACH_History/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*ACH_History_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTSPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
  
FROM @ETL.INBOUND/SRC/Backfill/LoanPaymentSPay/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanPaymentSPay_Backfill.csv*';

COPY INTO STG.SRC_DOCUWARELOANDOC_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND/SRC/Backfill/DocuwareLoanDoc/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*DocuwareLoanDoc_Backfill.csv*';

COPY INTO STG.SRC_COLLECTIONMOVEMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    to_timestamp_ntz($2, 'YYYY-MM-DD HH24:MI:SS'),
    $3,
    $4,
    to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS'),
    $6,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND/SRC/Backfill/CollectionMovement/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*CollectionMovement_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONPRODUCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    to_timestamp_ntz($14, 'YYYY-MM-DD HH24:MI:SS.FF'),
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22
  
FROM @ETL.INBOUND/SRC/Backfill/LoanApplicationProduct/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanApplicationProduct_Backfill.csv*';

COPY INTO STG.SRC_CREDITVENDORDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    to_timestamp_ntz($2, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
    $21
  
FROM @ETL.INBOUND/SRC/Backfill/CreditVendorData/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*CreditVendorData_Backfill.csv*';

COPY INTO STG.SRC_CREDITCARDATTEMPTS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
    $1,
    $2,
    to_timestamp_ntz($3, 'YYYY-MM-DD HH24:MI:SS.FF'),
    $4,
    $5,
    $6,
    $7,
    $8
  
FROM @ETL.INBOUND/SRC/Backfill/CreditCardAttempts/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*CreditCardAttempts_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
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
    CASE WHEN $38 = '' THEN NULL
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $39 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $40 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $41 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $42 = 'True' THEN 1
    ELSE 0
    END,
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
    CASE WHEN $54 = '' THEN NULL
    ELSE to_timestamp_ntz($54, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $55 = '' THEN NULL
    ELSE to_timestamp_ntz($55, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $56 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $57 = 'True' THEN 1
    ELSE 0
    END,
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
    CASE WHEN $70 = '' THEN NULL
    ELSE to_timestamp_ntz($70, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $71 = '' THEN NULL
    ELSE to_timestamp_ntz($71, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $72, 
    $73, 
    CASE WHEN $74 = '' THEN NULL
    ELSE $74
    END, 
    CASE WHEN $75 = '' THEN NULL
    ELSE $75
    END, 
    CASE WHEN $76 = '' THEN NULL
    ELSE $76
    END, 
    CASE WHEN $77 = '' THEN NULL
    ELSE $77
    END, 
    CASE WHEN $78 = '' THEN NULL
    ELSE $78
    END, 
    CASE WHEN $79 = '' THEN NULL
    ELSE $79
    END, 
    CASE WHEN $80 = '' THEN NULL
    ELSE $80
    END, 
    CASE WHEN $81 = '' THEN NULL
    ELSE $81
    END, 
    CASE WHEN $82 = '' THEN NULL
    ELSE $82
    END, 
    CASE WHEN $83 = '' THEN NULL
    ELSE $83
    END, 
    CASE WHEN $84 = '' THEN NULL
    ELSE $84
    END, 
    CASE WHEN $85 = '' THEN NULL
    ELSE $85
    END, 
    CASE WHEN $86 = '' THEN NULL
    ELSE $86
    END, 
    $87, 
    $88, 
    $89, 
    $90, 
    $91, 
    CASE WHEN $92 = 'True' THEN 1
    ELSE 0
    END, 
    $93, 
    CASE WHEN $94 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $95 = '' THEN NULL
    ELSE to_timestamp_ntz($95, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $96,
    $97,
    $98,
    CASE WHEN $99 = '' THEN NULL
    ELSE $99
    END,
    CASE WHEN $100 = '' THEN NULL
    ELSE to_timestamp_ntz($100, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $101 = '' THEN NULL
    ELSE to_timestamp_ntz($101, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $102,
    CASE WHEN $103 = '' THEN NULL
    ELSE to_timestamp_ntz($103, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $104 = '' THEN NULL
    ELSE to_timestamp_ntz($104, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $105 = '' THEN NULL
    ELSE to_timestamp_ntz($105, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $106 = '' THEN NULL
    ELSE to_timestamp_ntz($106, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $107, 
    CASE WHEN $108 = 'True' THEN 1
    ELSE 0
    END, 
    $109, 
    CASE WHEN $110 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $111 = '' THEN NULL
    ELSE to_timestamp_ntz($111, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $112 = '' THEN NULL
    ELSE $112
    END, 
    CASE WHEN $113 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $114 = '' THEN NULL
    ELSE $114
    END, 
    CASE WHEN $115 = '' THEN NULL
    ELSE $115
    END, 
    CASE WHEN $116 = 'True' THEN 1
    ELSE 0
    END, 
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
    $133, 
    $134, 
    $135, 
    $136, 
    CASE WHEN $137 = '' THEN NULL
    ELSE to_timestamp_ntz($137, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $138, 
    $139, 
    $140, 
    $141, 
    $142, 
    $143, 
    CASE WHEN $144 = 'True' THEN 1
    ELSE 0
    END, 
    $145, 
    $146
  
FROM @ETL.INBOUND/SRC/Backfill/OpenEndLoan/)

FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= '.*OpenEndLoan_Backfill.csv*';

COPY INTO STG.SRC_WEBLEAD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
		$3,
		$4,
		$5,
		to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
		$7,
		$8,
		$9,
		$10,
		$11,
		$12
   
FROM @ETL.INBOUND/SRC/Backfill/WebLead/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*WebLead_Backfill.csv*';

COPY INTO STG.SRC_VISITORCOMMUNICATIONCONSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
		$3,
		to_timestamp_ntz($4, 'YYYY-MM-DD HH24:MI:SS.FF'),
		$5,
		to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
		$7,
		$8,
		to_timestamp_ntz($9, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM'),
		$10
   
FROM @ETL.INBOUND/SRC/Backfill/VisitorCommunicationConsent/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*VisitorCommunicationConsent_Backfill.csv*';

COPY INTO STG.SRC_MARKETINGINVITATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
		to_date($33),
        to_date($34),
		$35,
		$36,
		$37,
        $38,
		$39,
		$40,
		$41,
		$42,
		$43
  
FROM @ETL.INBOUND/SRC/Backfill/MarketingInvitation/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MarketingInvitation_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
        $3,
		$4,
		$5,
		$6,
		$7,
		$8,
		$9
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetailCash/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailCash_Backfill.csv*';

COPY INTO STG.SRC_TRANSPOS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
        to_timestamp_ntz($3, 'YYYY-MM-DD HH24:MI:SS.FF'),
		$4,
		$5,
		$6,
		$7,
		$8,
		$9
  
FROM @ETL.INBOUND/SRC/Backfill/TransPOS/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransPOS_Backfill.csv*';

COPY INTO STG.SRC_MPAYAMORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
        $3,
        to_timestamp_ntz($4, 'YYYY-MM-DD HH24:MI:SS.FF'),
		$5,
		$6,
		$7,
		$8,
		$9,
        $10,
		$11,
		$12,
		to_date($13),
		$14,
		$15,
		$16
  
FROM @ETL.INBOUND/SRC/Backfill/MPayAmort/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayAmort_Backfill.csv*';

COPY INTO STG.SRC_MPAYRECALCINTERESTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'),
		to_date($6),
		to_date($7),
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
  
FROM @ETL.INBOUND/SRC/Backfill/MPayRecalcInterestAdj/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayRecalcInterestAdj_Backfill.csv*';

COPY INTO STG.SRC_SPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
		$2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'),
		to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
		$7,
		$8,
		$9,
        $10,
		$11,
		$12
  
FROM @ETL.INBOUND/SRC/Backfill/SPayInterest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*SPayInterest_Backfill.csv*';

COPY INTO STG.SRC_MPAYLOANINSYNCADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        to_timestamp_ntz($2, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
        to_timestamp_ntz($14, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($15, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $16,
        to_timestamp_ntz($17, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $18,
        to_timestamp_ntz($19, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($20, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $21,
        to_timestamp_ntz($22, 'YYYY-MM-DD HH24:MI:SS.FF'),   
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
        to_timestamp_ntz($33, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($34, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $35,    
        to_timestamp_ntz($36, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $37,
        to_timestamp_ntz($38, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($39, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $40,
        to_timestamp_ntz($41, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
        CASE WHEN $74 = '' THEN NULL
        ELSE $74
        END, 
        CASE WHEN $75 = '' THEN NULL
        ELSE $75
        END, 
        CASE WHEN $76 = '' THEN NULL
        ELSE $76
        END, 
        CASE WHEN $77 = '' THEN NULL
        ELSE $77
        END, 
        CASE WHEN $78 = '' THEN NULL
        ELSE $78
        END, 
        CASE WHEN $79 = '' THEN NULL
        ELSE $79
        END, 
        CASE WHEN $80 = '' THEN NULL
        ELSE $80
        END, 
        CASE WHEN $81 = '' THEN NULL
        ELSE $81
        END, 
        CASE WHEN $82 = '' THEN NULL
        ELSE $82
        END, 
        $83, 
        $84, 
        CASE WHEN $85 = '' THEN NULL
        ELSE $85
        END, 
        CASE WHEN $86 = '' THEN NULL
        ELSE $86
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
        $97,
        $98,
        $99
        
FROM @ETL.INBOUND/SRC/Backfill/MPayLoanInSyncAdj/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayLoanInSyncAdj_Backfill.csv*';

COPY INTO STG.SRC_VISITORCOMMUNICATIONPREFERENCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM')
        
FROM @ETL.INBOUND/SRC/Backfill/VisitorCommunicationPreference/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*VisitorCommunicationPreference_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTMPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
        $24
        
FROM @ETL.INBOUND/SRC/Backfill/LoanPaymentMPay/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanPaymentMPay_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANSTATEMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        to_timestamp_ntz($14, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $15,
        $16,
        to_timestamp_ntz($17, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $18,
        $19,
        $20,
        to_timestamp_ntz($21, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $22,
        $23,    
        $24,
        $25,
        $26,
        to_timestamp_ntz($27, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $28,
        $29,
        $30,
        $31,
        $32,
        to_timestamp_ntz($33, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
        $60, 
        $61, 
        $62,
        $63, 
        $64,
        to_timestamp_ntz($65, 'YYYY-MM-DD HH24:MI:SS.FF'), 
        $66, 
        $67, 
        $68, 
        to_date($69), 
        to_timestamp_ntz($70, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM'), 
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
        $92
  
FROM @ETL.INBOUND/SRC/Backfill/OpenEndLoanStatement/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*OpenEndLoanStatement_Backfill.csv*';

COPY INTO STG.SRC_LOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
        to_timestamp_ntz($32, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($33, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $34,
        to_date($35),    
        $36,
        to_timestamp_ntz($37, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $38,
        $39,
        to_timestamp_ntz($40, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $41,
        $42,
        to_timestamp_ntz($43, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $44,
        $45,
        $46,
        to_date($47),
        $48,
        to_timestamp_ntz($49, 'YYYY-MM-DD HH24:MI:SS.FF'), 
        $50, 
        $51, 
        to_timestamp_ntz($52, 'YYYY-MM-DD HH24:MI:SS.FF'), 
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
        to_date($69), 
        to_date($70),
        to_date($71), 
        $72,
        $73, 
        $74, 
        to_date($75), 
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
        $90
  
FROM @ETL.INBOUND/SRC/Backfill/Loan/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*Loan_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTOPENEND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
        $52, 
        $53,
        $54, 
        $55,
        $56
  
FROM @ETL.INBOUND/SRC/Backfill/LoanPaymentOpenEnd/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*LoanPaymentOpenEnd_Backfill.csv*';

COPY INTO STG.SRC_AACBEXPORTDATAARCHIVE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        CASE WHEN $1 = '' THEN NULL
        ELSE $1
        END,
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
        to_timestamp_ntz($30, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $31
  
FROM @ETL.INBOUND/SRC/Backfill/AACbExportDataArchive/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*AACbExportDataArchive_Backfill.csv*';

COPY INTO STG.SRC_OPENENDINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'), 
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
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
  
FROM @ETL.INBOUND/SRC/Backfill/OpenEndInterest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*OpenEndInterest_Backfill.csv*';

COPY INTO STG.SRC_DOCUWAREID_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        to_timestamp_ntz($2, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM'),
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        to_timestamp_ntz($9, 'YYYY-MM-DD HH24:MI:SS.FF')
  
FROM @ETL.INBOUND/SRC/Backfill/DocuwareID/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*DocuwareID_Backfill.csv*';

COPY INTO STG.SRC_DOCUWAREDOCUMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        to_timestamp_ntz($2, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $3,
        $4,
        $5,
        $6,
        to_date($7),
        $8,
        $9
  
FROM @ETL.INBOUND/SRC/Backfill/DocuwareDocument/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*DocuwareDocument_Backfill.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_1.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_2.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_3.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_4.csv*';

COPY INTO STG.SRC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
        $11
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetail_Backfill_1.csv*';

COPY INTO STG.SRC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
        $11
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetail_Backfill_2.csv*';

COPY INTO STG.SRC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
        $11
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetail_Backfill_3.csv*';

COPY INTO STG.SRC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
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
        $11
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetail_Backfill_4.csv*';

COPY INTO STG.SRC_ENDOFDAYINVENTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9
  
FROM @ETL.INBOUND/SRC/Backfill/EndOfDayInventoryDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*EndOfDayInventoryDetail_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetailAcct/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailAcct_Backfill_1.csv*';

COPY INTO STG.SRC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetailAcct/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailAcct_Backfill_2.csv*';

COPY INTO STG.SRC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetailAcct/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailAcct_Backfill_3.csv*';

COPY INTO STG.SRC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetailAcct/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailAcct_Backfill_4.csv*';

COPY INTO STG.SRC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14
  
FROM @ETL.INBOUND/SRC/Backfill/MPayInterest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayInterest_Backfill_1.csv*';

COPY INTO STG.SRC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14
  
FROM @ETL.INBOUND/SRC/Backfill/MPayInterest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayInterest_Backfill_2.csv*';

COPY INTO STG.SRC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14
  
FROM @ETL.INBOUND/SRC/Backfill/MPayInterest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayInterest_Backfill_3.csv*';

COPY INTO STG.SRC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-19'),
        $1,
        $2,
        $3,
        $4,
        to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF'),
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FF'),
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14
  
FROM @ETL.INBOUND/SRC/Backfill/MPayInterest/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*MPayInterest_Backfill_4.csv*';