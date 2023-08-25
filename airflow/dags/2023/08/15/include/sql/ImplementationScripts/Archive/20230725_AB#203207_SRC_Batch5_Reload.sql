COPY INTO STG.SRC_WEBDIALERUSER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
    $1,
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND/SRC/Backfill/WebDialerUser/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH1_EON_GZ)
pattern= '.*WebDialerUser_Backfill.csv*';

COPY INTO STG.SRC_VISITORCOMMUNICATIONPREFERENCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
        $1,
        $2,
        $3,
        $4,
        $5,
        to_timestamp_ntz($6, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM')
        
FROM @ETL.INBOUND/SRC/Backfill/VisitorCommunicationPreference/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*VisitorCommunicationPreference_Backfill.csv*';

COPY INTO STG.SRC_VISITORCOMMUNICATIONCONSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_TRANSPOS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_TRANSDETAILCHECK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_TRANSDETAILCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6
  
FROM @ETL.INBOUND/SRC/Backfill/TransDetailAcct/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*TransDetailAcct_Backfill_4.csv*';

COPY INTO STG.SRC_SPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_PAYDAYLOANAPPROVAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_OPENENDLOANSTATEMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_OPENENDINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_MPAYRECALCINTERESTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-12'),
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

COPY INTO STG.SRC_MPAYLOANINSYNCADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-12'),
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

COPY INTO STG.SRC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_MPAYAMORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_MARKETINGINVITATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-12'),
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

COPY INTO STG.SRC_LOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_LOANPAYMENTOPENENDSTREAM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_LOANPAYMENTMPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_LOANPAYMENTDUEDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH1_EON_GZ)
pattern= '.*LoanPaymentDueDate_Backfill.csv*';

COPY INTO STG.SRC_LOANDOCPRINTED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_LOANAPPLICATIONPRODUCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_LOANAPPLICATIONINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_ENDOFDAYINVENTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_DOCUWAREID_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_CREDITVENDORDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

COPY INTO STG.SRC_CREDITREPORTINGLOANACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH1_EON_GZ)
pattern= '.*CreditReportingLoanActivity_Backfill.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_1.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_2.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_3.csv*';

COPY INTO STG.SRC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
        $1,
        $2,
        $3
  
FROM @ETL.INBOUND/SRC/Backfill/BalSheet_TransDetail/)

FILE_FORMAT = ( FORMAT_NAME = STG.SRC_CSV_PIPE_SH0_EON_GZ)
pattern= '.*BalSheet_TransDetail_Backfill_4.csv*';

COPY INTO STG.SRC_AACBEXPORTDATAARCHIVE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-07-25'),
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