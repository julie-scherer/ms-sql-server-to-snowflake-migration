COPY INTO STG.CM_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,
    $2,
    $3
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/BalSheet_TransDetail/BALSHEET_TRANSDETAIL_MAX.csv.gz*';

COPY INTO STG.CM_ENDOFDAYINVENTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
	$1,  
    $2,  
    $3,  
    $4, 
    $5,  
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END AS CAB_FEE_RECEIVABLE_AMT,  
    $7,  
    $8,  
    $9
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/EndOfDayInventoryDetail/EndOfDayInventoryDetail_MAX.csv.gz.*';

COPY INTO STG.CM_LOANINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/LoanIncome/LoanIncome_MAX.csv.gz.*';

COPY INTO STG.CM_LOANPAYMENTMPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12,  $13,  $14,  $15,  $16,  $17,  $18,  $19,  $20,  $21,  $22,  $23,  $24
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/LOANPAYMENTMPAY/LOANPAYMENTMPAY_MAX.csv.gz.*';

COPY INTO STG.CM_LOANPAYMENTOPENEND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12,  $13,  $14,  $15,  $16,  $17,  $18,  $19,  $20,  $21,  $22,  $23,  $24,  $25,  
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END AS FUNDING_ACH_HISTORY_KEY,  
    $27,  $28,  $29,  $30,  $31,  $32,  $33,  $34,  
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END AS ACH_PROCESSING_QUEUE_KEY,  
    $36,  $37,  $38,  $39,  $40,  $41,  $42,  $43,  $44,  $45,  $46,  $47,  $48,  $49,  $50,  $51,  $52,  $53,  $54,  $55,  $56
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/LoanPaymentOpenEnd/LOANPAYMENTOPENEND_MAX.csv.gz.*';

COPY INTO STG.CM_LOANPAYMENTSPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12,  $13
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/LOANPAYMENTSPAY/LOANPAYMENTSPAY_MAX.csv.gz.*';

COPY INTO STG.CM_MPAYAMORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  
    CASE WHEN $4 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($4, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12, 
    CASE WHEN $13 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($13, 'mm/dd/yyyy hh12:mi:ss am'))
    END, $14,  $15,  $16
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/MPayAmort/MPAYAMORT_MAX.csv.gz.*';

COPY INTO STG.CM_MPAYLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12,  $13,  
    CASE WHEN $14 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($14, 'mm/dd/yyyy hh12:mi:ss am'))
    END,
    $15, 
    CASE WHEN $16 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($16, 'mm/dd/yyyy hh12:mi:ss am'))
    END, 
    $17, 
    CASE WHEN $18 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($18, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $19,  $20,  
    CASE WHEN $21 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($21, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $22,  $23,  $24,  $25,  
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END,  
    $27,  $28,  $29,  $30,  $31,  $32,  $33,  $34,  $35,  $36,  $37,  $38,  $39,  $40,  $41,  $42,  $43,  $44,  $45,  $46,  $47,  
    CASE WHEN $48 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($48, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $49,  
    CASE WHEN $50 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($50, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $51,  $52,  $53,  $54,  $55,  $56,  $57,  $58,  
    CASE WHEN $59 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($59, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    CASE WHEN $60 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($60, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $61,  $62,  $63,  $64,  
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END,  
    $66,  $67,  $68,  $69,  $70,  $71,  $72,  $73,  $74,  $75,  $76,  $77,  $78,  $79,  $80,  $81
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/MPayLoan/MPAYLOAN_MAX.csv.gz.*';


COPY INTO STG.CM_PRESENTMENTREQUESTACHHISTORYXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/PresentmentRequestACHHistoryXRef/PRESENTMENTREQUESTACHHISTORYXREF_MAX.csv.gz.*';

COPY INTO STG.CM_TRANSCODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/TransCode/TRANSCODE_MAX.csv.gz.*';

COPY INTO STG.CM_RBCEFUNDBATCHDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12,  
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END AS RBC_EFUND_BATCH_SUMMARY_KEY,  
    $14,  $15
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/RbcEFundBatchDetail/RBCEFUNDBATCHDETAIL_MAX.csv.gz.*';

COPY INTO STG.CM_RISREPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    CASE WHEN $1 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($1, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $2,  $3,  $4,  $5,  
    CASE WHEN $6 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($6, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $7,  $8,  
    CASE WHEN $9 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($9, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $10,  $11,  
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,  
    $13,  $14,  $15,  $16,  $17,  $18,  
    CASE WHEN $19 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($19, 'mm/dd/yyyy hh12:mi:ss am'))
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
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($25, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END,  
    $27,  $28,  $29,  $30,  $31,  
    CASE WHEN $32 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($32, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $33,  $34,  
    CASE WHEN $35 = '' THEN NULL
    ELSE TO_DATE(TO_TIMESTAMP_NTZ($35, 'mm/dd/yyyy hh12:mi:ss am'))
    END,  
    $36,  $37
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/RISREPT/RISREPT_MAX.csv.gz.*';

select count(*) from STG.CM_SPAYLOAN_HIST;

COPY INTO STG.CM_SPAYLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  $10,  $11,  $12,  $13,  $14,  $15,  $16,  $17,  $18,  $19,  $20,  $21,  $22,  $23,  $24,  $25,  $26,  $27,  $28,  $29,  $30,  $31,  $32
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/SPAYLOAN/SPAYLOAN_MAX.csv.gz.*';

COPY INTO STG.CM_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/TransDetailAcct/TRANSDETAILACCT_MAX.csv.gz.*';

COPY INTO STG.CM_OPENENDLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-02-26'),
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,  
    $9,  $10,  $11,  $12,  $13,  $14,  $15,  $16,  $17,  $18,  $19,  $20,  $21,  $22,  $23,  $24,  $25,  $26,  $27,  $28,  $29,  $30,  $31,  $32,  $33,  $34,  $35,  $36,  $37,  $38,  $39,  $40,  $41,  $42,  $43,  $44,  $45,  $46,  $47,  $48,  $49,  $50,  $51,  $52,  $53,  $54,  $55,  $56,  $57,  $58,  $59,  $60,  $61,  $62,  $63,  $64,  $65,  $66,  $67,  $68,  $69,  $70,  $71,  $72,  $73,  $74,  $75,  $76,  $77,  $78,  $79,  $80,  $81,  $82,  $83,  $84,  $85,  $86,  $87,  $88,  $89,  $90,  $91,  $92,  $93,  $94,  $95,  $96,  $97,  $98,  $99,  $100,  $101,  $102,  $103,  $104,  $105,  $106,  $107,  $108,  $109,  $110,  $111,  $112,  $113,  $114,  $115,  $116,  $117,  $118,  $119,  $120,  $121,  $122,  $123,  $124,  $125,  $126,  $127,  $128,  $129,  $130,  $131,  $132,  $133,  $134,  $135,  $136,  $137,  $138,  $139,  $140,  $141,  $142,  $143,  $144,  $145,  $146
    from @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.CM_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/OpenEndLoan/OPENENDLOAN_MAX.csv.gz.*';
