COPY INTO STG.SRC_OPENENDRECALCINTERESTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
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
pattern= 'inbound/SRC/Backfill/OpenEndRecalcInterestAdj/OpenEndRecalcInterestAdj_Backfill.csv*';

COPY INTO STG.SRC_LOANFUNDINGHISTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFundingHistoryDetail/LoanFundingHistoryDetail_Backfill.csv*';

COPY INTO STG.SRC_WEBDIALERRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $8, 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    $18,
    $19,
    $20,
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $24,
    $25,
    $26,
    CASE WHEN $27 = '' THEN NULL
    ELSE $27
    END,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    CASE WHEN $30 = '' THEN NULL
    ELSE $30
    END,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    $32,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
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
    ELSE $38
    END,
    $39,
    $40,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    CASE WHEN $42 = '' THEN NULL
    ELSE $42
    END,
    CASE WHEN $43 = '' THEN NULL
    ELSE to_timestamp_ntz($43, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    CASE WHEN $46 = 'True' THEN 1
    ELSE 0
    END,
    $47,
    $48,
    CASE WHEN $49 = '' THEN NULL
    ELSE $49
    END,
    $50,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    $52,
    $53,
    $54,
    $55,
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END,
    $57,
    CASE WHEN $58 = '' THEN NULL
    ELSE $58
    END,
    $59,
    $60,
    $61,
    $62,
    $63,
    $64,
    $65,
    CASE WHEN $66 = '' THEN NULL
    ELSE $66
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE to_timestamp_ntz($67, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $68,
    $69
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerResult/WebDialerResult_Backfill.csv*';

COPY INTO STG.SRC_LOANDUEDATECHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDueDateChange/LoanDueDateChange_Backfill.csv*';

COPY INTO STG.SRC_TITLELOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $11,
    $12,
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
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    $23,
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0
    END,
    $25,
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END,
    CASE WHEN $27 = '' THEN NULL
    ELSE $27
    END,
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
    CASE WHEN $45 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $46 = 'True' THEN 1
    ELSE 0
    END,
    $47,
    $48,
    $49,
    $50,
    $51,
    $52,
    $53,
    $54,
    $55,
    CASE WHEN $56 = 'True' THEN 1
    ELSE 0
    END,
    $57,
    $58,
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END,
    CASE WHEN $60 = '' THEN NULL
    ELSE $60
    END,
    $61,
    $62,
    $63,
    $64,
    $65,
    CASE WHEN $66 = '' THEN NULL
    ELSE to_timestamp_ntz($66, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $67 = 'True' THEN 1
    ELSE 0
    END,
    $68,
    CASE WHEN $69 = 'True' THEN 1
    ELSE 0
    END,
    $70,
    CASE WHEN $71 = '' THEN NULL
    ELSE to_timestamp_ntz($71, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $72,
    CASE WHEN $73 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TitleLoan/TitleLoan_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILSERVICE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransDetailService/TransDetailService_Backfill.csv*';

COPY INTO STG.SRC_WEBLEADPOSTDATAARCHIVE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $9,
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    $13
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLeadPostDataArchive/WebLeadPostDataArchive_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANCYCLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $11,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    $13,
    $14
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndLoanCycle/OpenEndLoanCycle_Backfill.csv*';

COPY INTO STG.SRC__ACHPRESENTMENT1904CONVERT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    $3,
    $4,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_AchPresentment1904Convert/_AchPresentment1904Convert_Backfill.csv*';

COPY INTO STG.SRC_LOANINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanIncome/LoanIncome_Backfill.csv*';

COPY INTO STG.SRC_WEBDIALERUPLOADHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebDialerUploadHistory/WebDialerUploadHistory_Backfill.csv*';

COPY INTO STG.SRC_PROMISETOPAYDETAILEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7,
    $8,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PromiseToPayDetailEdit/PromiseToPayDetailEdit_Backfill.csv*';

COPY INTO STG.SRC_CALLCAMPAIGNQUEUEACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    CASE WHEN $14 = '' THEN NULL
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $16,
    $17,
    $18
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignQueueActivity/CallCampaignQueueActivity_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLAPPLICATIONSTATUSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallApplicationStatusHistory/WebCallApplicationStatusHistory_Backfill.csv*';

COPY INTO STG.SRC_OENDLOANINSYNCADJDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
pattern= 'inbound/SRC/Backfill/OEndLoanInSyncAdjDetail/OEndLoanInSyncAdjDetail_Backfill.csv*';

COPY INTO STG.SRC_CASHEDCHECKPAYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    $12
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CashedCheckPayment/CashedCheckPayment_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLWORKITEMCATEGORYHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallWorkItemCategoryHistory/WebCallWorkItemCategoryHistory_Backfill.csv*';

COPY INTO STG.SRC_BALSHEETCOLUMNS2_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
pattern= 'inbound/SRC/Backfill/BalSheetColumns2/BalSheetColumns2_Backfill.csv*';

COPY INTO STG.SRC_RIS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    to_timestamp_ntz($1, 'MM/DD/YYYY HH12:MI:ss AM'),
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    $17,
    $18,
    $19,
    CASE WHEN $20 = '' THEN NULL
    ELSE $20
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RIS/RIS_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMEREMPLOYER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
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
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $13,
    $14,
    $15,
    $16,
    $17,
    CASE WHEN $18 = '' THEN NULL
    ELSE to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $19,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    $21,
    $22,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM'),
    $25,
    CASE WHEN $26 = '' THEN NULL
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $27,
    CASE WHEN $28 = '' THEN NULL
    ELSE to_timestamp_ntz($28, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $29,
    CASE WHEN $30 = '' THEN NULL
    ELSE to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $31,
    CASE WHEN $32 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerEmployer/CustomerEmployer_Backfill.csv*';

COPY INTO STG.SRC_LOANDEPOSITSTATUSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
pattern= 'inbound/SRC/Backfill/LoanDepositStatusHistory/LoanDepositStatusHistory_Backfill.csv*';

COPY INTO STG.SRC_VISITORAUTHDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorAuthData/VisitorAuthData_Backfill.csv*';

COPY INTO STG.SRC_VISITOR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $15
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Visitor/Visitor_Backfill.csv*';

COPY INTO STG.SRC_CREDITCARDSEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardsEdit/CreditCardsEdit_Backfill.csv*';

COPY INTO STG.SRC_CCARDRESPONSES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CCardResponses/CCardResponses_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMEREMPLOYEREDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerEmployerEdit/CustomerEmployerEdit_Backfill.csv*';

COPY INTO STG.SRC_TELLERLOGIN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TellerLogin/TellerLogin_Backfill.csv*';

COPY INTO STG.SRC_OPENENDINTERESTSTREAM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
pattern= 'inbound/SRC/Backfill/OpenEndInterestStream/OpenEndInterestStream_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTSPASTDUEDETAILBACKFILL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentsPastDueDetailBackfill/PaymentsPastDueDetailBackfill_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTDUEDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentDueDate/LoanPaymentDueDate_Backfill.csv*';

COPY INTO STG.SRC_VISITOREDITHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorEditHistory/VisitorEditHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONIDENTIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationIdentification/LoanApplicationIdentification_Backfill.csv*';

COPY INTO STG.SRC_VAULTMGRAUTHORIZATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultMgrAuthorization/VaultMgrAuthorization_Backfill.csv*';


COPY INTO STG.SRC_LOANFUNDING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0 
    END,
    $14
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanFunding/LoanFunding_Backfill.csv*';

COPY INTO STG.SRC_LOANAUTHORIZEDPAYMENTMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
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
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END,
    $9,
    $10,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0 
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END,
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0 
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanAuthorizedPaymentMethod/LoanAuthorizedPaymentMethod_Backfill.csv*';

COPY INTO STG.SRC_OPTPLUSRDFACCOUNTCARD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    $13,
    $14,
    $15,
    $16,
    $17,
    $18
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OptPlusRDFAccountCard/OptPlusRDFAccountCard_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
pattern= 'inbound/SRC/Backfill/TransDetailLoan/TransDetailLoan_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterResult/FormLetterResult_Backfill.csv*';

COPY INTO STG.SRC_LOANSTATUSCHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0 
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0 
    END,
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0 
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanStatusChange/LoanStatusChange_Backfill.csv*';

COPY INTO STG.SRC_OPENENDLOANSTATEMENTHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    $9,
    $10,
    $11,
    $12, 
    $13, 
    to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $15, 
    $16, 
    CASE WHEN $17 = '' THEN NULL
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $18, 
    $19, 
    $20, 
    CASE WHEN $21 = '' THEN NULL
    ELSE to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $22, 
    $23, 
    $24, 
    $25, 
    $26, 
    CASE WHEN $27 = '' THEN NULL
    ELSE to_timestamp_ntz($27, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $28, 
    $29, 
    $30, 
    $31, 
    $32, 
    CASE WHEN $33 = '' THEN NULL
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
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
    CASE WHEN $57 = 'True' THEN 1
    ELSE 0
    END, 
    $58, 
    $59, 
    $60, 
    $61, 
    $62, 
    $63, 
    CASE WHEN $64 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $65 = '' THEN NULL
    ELSE to_timestamp_ntz($65, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $66, 
    $67, 
    CASE WHEN $68 = '' THEN NULL
    ELSE $68
    END, 
    CASE WHEN $69 = '' THEN NULL
    ELSE to_timestamp_ntz($69, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $70 = '' THEN NULL
    ELSE to_timestamp_ntz($70, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    CASE WHEN $71 = '' THEN NULL
    ELSE $71
    END, 
    CASE WHEN $72 = '' THEN NULL
    ELSE $72
    END, 
    CASE WHEN $73 = '' THEN NULL
    ELSE $73
    END, 
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
    CASE WHEN $92 = '' THEN NULL
    ELSE $92
    END, 
    CASE WHEN $93 = '' THEN NULL
    ELSE to_timestamp_ntz($93, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $94 = '' THEN NULL
    ELSE to_timestamp_ntz($94, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndLoanStatementHistory/OpenEndLoanStatementHistory_Backfill.csv*';

COPY INTO STG.SRC_MPAYLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    CASE WHEN $14 = '' THEN NULL
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $15,  
    CASE WHEN $16 = '' THEN NULL
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $17,
    CASE WHEN $18 = '' THEN NULL
    ELSE to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $19, 
    $20, 
    CASE WHEN $21 = '' THEN NULL
    ELSE to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END, 
    $23, 
    $24, 
    $25, 
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END, 
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END, 
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
    CASE WHEN $48 = '' THEN NULL
    ELSE to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $49, 
    CASE WHEN $50 = '' THEN NULL
    ELSE to_timestamp_ntz($50, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $51, 
    $52, 
    $53, 
    CASE WHEN $54 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $55 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $56 = 'True' THEN 1
    ELSE 0
    END, 
    $57, 
    CASE WHEN $58 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $59 = '' THEN NULL
    ELSE to_timestamp_ntz($59, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $60 = '' THEN NULL
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $61, 
    $62, 
    $63, 
    $64, 
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END, 
    $66, 
    $67, 
    $68, 
    $69, 
    $70, 
    CASE WHEN $71 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $72 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $73 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $74 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $75 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $76 = '' THEN NULL
    ELSE $76
    END, 
    CASE WHEN $77 = '' THEN NULL
    ELSE $77
    END, 
    CASE WHEN $78 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $79 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $80 = 'True' THEN 1
    ELSE 0
    END, 
    $81
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MPayLoan/MPayLoan_Backfill.csv*';

COPY INTO STG.SRC_CREDITCARDS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    $11,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $14 = '' THEN NULL
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $15,  
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $17 = '' THEN NULL
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $18, 
    $19, 
    $20, 
    $21, 
    $22, 
    $23, 
    $24, 
    $25, 
    CASE WHEN $26 = '' THEN NULL
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END, 
    $28, 
    $29, 
    $30, 
    $31, 
    $32, 
    $33,
    CASE WHEN $34 = 'True' THEN 1
    ELSE 0
    END, 
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
    $44, 
    $45
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCards/CreditCards_Backfill.csv*';

COPY INTO STG.SRC_PROMISETOPAYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11,
    $12, 
    $13, 
    $14, 
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,  
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
    $39, 
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END, 
    $41
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PromiseToPayDetail/PromiseToPayDetail_Backfill.csv*';

COPY INTO STG.SRC_ISSUER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
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
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END, 
    $14, 
    $15,  
    CASE WHEN $16 = '' THEN NULL
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
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
    CASE WHEN $31 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END, 
    CASE WHEN $33 = 'True' THEN 1
    ELSE 0
    END,
    $34, 
    $35, 
    $36, 
    $37, 
    $38, 
    $39
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Issuer/Issuer_Backfill.csv*';

COPY INTO STG.SRC_RISREPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    CASE WHEN $1 = '' THEN NULL
    ELSE to_timestamp_ntz($1, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $10,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END, 
    $13, 
    $14, 
    $15,  
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END,
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
    CASE WHEN $32 = '' THEN NULL
    ELSE to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $33,
    CASE WHEN $34 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $35 = '' THEN NULL
    ELSE to_timestamp_ntz($35, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $36, 
    $37
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RISREPT/RISREPT_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERPRINTED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
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
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END, 
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END, 
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END, 
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,  
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterPrinted/FormLetterPrinted_Backfill.csv*';

COPY INTO STG.SRC_VISITORDOCUMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $7,
    $8,
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END, 
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VisitorDocument/VisitorDocument_Backfill.csv*';

COPY INTO STG.SRC_PRESENTMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Presentment/Presentment_Backfill.csv*';

COPY INTO STG.SRC_TRANSDETAILCASHPARSEDCASH FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransDetailCashParsedCash/TransDetailCashParsedCash_Backfill.csv*';

COPY INTO STG.SRC_MPAYRECALCLOANPAYMENTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    $18,
    $19,
    $20,
    $21,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    $23,
    $24,
    $25,
    $26,
    CASE WHEN $27 = '' THEN NULL
    ELSE $27
    END,
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
    $32,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END,
    CASE WHEN $36 = '' THEN NULL
    ELSE to_timestamp_ntz($36, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $37 = '' THEN NULL
    ELSE to_timestamp_ntz($37, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $38 = '' THEN NULL
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $39 = '' THEN NULL
    ELSE to_timestamp_ntz($39, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $40,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    CASE WHEN $42 = '' THEN NULL
    ELSE $42
    END,
    $43,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    $46,
    $47,
    $48,
    CASE WHEN $49 = '' THEN NULL
    ELSE $49
    END,
    $50,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    $52,
    $53,
    $54,
    $55,
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END,
    $57,
    CASE WHEN $58 = '' THEN NULL
    ELSE $58
    END,
    $59,
    $60,
    $61
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MPayRecalcLoanPaymentAdj/MPayRecalcLoanPaymentAdj_Backfill.csv*';

COPY INTO STG.SRC_PAYDAYLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
    $1,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    $8, 
    $9, 
    $10,
    $11,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    $13,
    $14,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaydayLoan/PaydayLoan_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
    $13,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,    
    $18
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerIncome/CustomerIncome_Backfill.csv*';

COPY INTO STG.SRC_SERVICETRANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-06-22'),
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
    $17
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ServiceTrans/ServiceTrans_Backfill.csv*';

