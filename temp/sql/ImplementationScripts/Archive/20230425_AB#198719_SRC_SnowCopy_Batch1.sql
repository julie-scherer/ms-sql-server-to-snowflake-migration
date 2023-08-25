COPY INTO STG.SRC__NEXTSTATEMENTFIXES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    $8,
    $9,
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    $13,
    to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $17 = '' THEN NULL 
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $18 = '' THEN NULL 
    ELSE to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $19,
    $20,
    $21,
    $22

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_NextStatementFixes/_NextStatementFixes_Backfill.csv*';

COPY INTO STG.SRC__CH035718_DOCLOST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CH035718_DOCLOST/_CH035718_DOCLOST_Backfill.csv*';

COPY INTO STG.SRC_BANKCLOSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BankClosed/BankClosed_Backfill.csv*';

COPY INTO STG.SRC_OPENENDQUEUELOANCYCLEUPDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    $6,
    $7, 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $9, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OpenEndQueueLoanCycleUpdate/OpenEndQueueLoanCycleUpdate_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlan/PaymentPlan_Backfill.csv*';

COPY INTO STG.SRC_GOLDCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    $45

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldConfig/GoldConfig_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGINTERESTRATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/LoanProductConfigInterestRate/LoanProductConfigInterestRate_Backfill.csv*';

COPY INTO STG.SRC_CREDITREPORTINGL1SEGMENTHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditReportingL1SegmentHistory/CreditReportingL1SegmentHistory_Backfill.csv*';

COPY INTO STG.SRC_INCOMETYPELOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeTypeLocation/IncomeTypeLocation_Backfill.csv*';

COPY INTO STG.SRC_DEPOSITBAGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DepositBagHistory/DepositBagHistory_Backfill.csv*';

COPY INTO STG.SRC_COMPANYCREDENTIAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CompanyCredential/CompanyCredential_Backfill.csv*';

COPY INTO STG.SRC_DISCOUNTMASTERLOANPRODUCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DiscountMasterLoanProduct/DiscountMasterLoanProduct_Backfill.csv*';

COPY INTO STG.SRC_SDNLIST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SDNList/SDNList_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGLOANSTATS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigLoanStats/LoanProductConfigLoanStats_Backfill.csv*';

COPY INTO STG.SRC_GLACCTHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctHistory/GLAcctHistory_Backfill.csv*';

COPY INTO STG.SRC_PROCESSCONFIGDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessConfigDetail/ProcessConfigDetail_Backfill.csv*';

COPY INTO STG.SRC_GLACCTLOANPRODUCTGROUPHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    to_timestamp_ntz($36,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($37,'MM/DD/YYYY HH12:MI:SS AM') 

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GLAcctLoanProductGroupHistory/GLAcctLoanProductGroupHistory_Backfill.csv*';

COPY INTO STG.SRC_SPECIALMESSAGELOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SpecialMessageLocation/SpecialMessageLocation_Backfill.csv*';

COPY INTO STG.SRC_CREDITCARDRESULTCODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    $12,
    CASE WHEN $13 = '' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $14,
    $15,
    $16,
    $17,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    $19,
    $20,
    $21,
    $22

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardResultCode/CreditCardResultCode_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGLOANFEERATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/LoanProductConfigLoanFeeRate/LoanProductConfigLoanFeeRate_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERFLASHORREBATES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerFlashORRebates/CustomerFlashORRebates_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLDUALAUTH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallDualAuth/WebCallDualAuth_Backfill.csv*';

COPY INTO STG.SRC_VAULTMASTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    $16, 
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END, 
    $18, 
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END, 
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END, 
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
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END, 
    CASE WHEN $57 = '' THEN NULL
    ELSE $57
    END, 
    CASE WHEN $58 = '' THEN NULL
    ELSE $58
    END, 
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultMaster/VaultMaster_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
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
    CASE WHEN $28 = '' THEN NULL 
    ELSE to_timestamp_ntz($28, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
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
pattern= 'inbound/SRC/Backfill/FormLetter/FormLetter_Backfill.csv*';

COPY INTO STG.SRC_PRODUCTOPENLOANMATRIX_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    $3,
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProductOpenLoanMatrix/ProductOpenLoanMatrix_Backfill.csv*';

COPY INTO STG.SRC_RETURNCHECKFILE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ReturnCheckFile/ReturnCheckFile_Backfill.csv*';

COPY INTO STG.SRC_FORMLETTERLOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/FormLetterLocation/FormLetterLocation_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGBUMPUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    $6, 
    $7,
    to_timestamp_ntz($8,'MM/DD/YYYY HH12:MI:SS AM'), 
    $9,
    $10,
    $11

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigBumpUp/LoanProductConfigBumpUp_Backfill.csv*';

COPY INTO STG.SRC_IDENTIFICATIONTYPESTATEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    $3, 
    $4, 
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'), 
    $6, 
    $7,
    $8,
    $9,
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
    to_timestamp_ntz($13,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($14,'MM/DD/YYYY HH12:MI:SS AM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IdentificationTypeStateHistory/IdentificationTypeStateHistory_Backfill.csv*';

COPY INTO STG.SRC_AUTOREPORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
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
    CASE WHEN $25 = '' THEN NULL 
    ELSE to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $26,
    CASE WHEN $27 = '' THEN NULL
    ELSE $27
    END,
    $28,
    CASE WHEN $29 = '' THEN NULL 
    ELSE to_timestamp_ntz($29, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')
    END,
    CASE WHEN $30 = '' THEN NULL 
    ELSE to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')
    END,
    CASE WHEN $31 = '' THEN NULL 
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')
    END,
    $32

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/AutoReport/AutoReport_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERCARDREVIEW_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerCardReview/CustomerCardReview_Backfill.csv*';

COPY INTO STG.SRC_MONEYGRAMLOOKUPLOG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')
    END,
    $6

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/MoneyGramLookupLog/MoneyGramLookupLog_Backfill.csv*';

COPY INTO STG.SRC_STORECLOSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    to_timestamp_ntz($2,'MM/DD/YYYY HH12:MI:SS AM'),
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/StoreClosed/StoreClosed_Backfill.csv*';

COPY INTO STG.SRC_WIRETRANSFERFILEIMPORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    to_timestamp_ntz($2,'MM/DD/YYYY HH12:MI:SS AM'),
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WireTransferFileImport/WireTransferFileImport_Backfill.csv*';

COPY INTO STG.SRC__VIRGINIADATEFIXES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($3,'MM/DD/YYYY HH12:MI:SS AM'),
    $4,
    $5,
    $6,
    $7, 
    $8, 
    to_timestamp_ntz($9,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($10,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($11,'MM/DD/YYYY HH12:MI:SS AM'),
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($13,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($14,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($15,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($16,'MM/DD/YYYY HH12:MI:SS AM'),
    $17
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_VirginiaDateFixes/_VirginiaDateFixes_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTENABLENEWLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    to_timestamp_ntz($9,'MM/DD/YYYY HH12:MI:SS AM'), 
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
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
    $17,
    $18,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductEnableNewLoan/LoanProductEnableNewLoan_Backfill.csv*';

COPY INTO STG.SRC_PROCESSCONFIGDETAILHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $7,
    $8

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ProcessConfigDetailHistory/ProcessConfigDetailHistory_Backfill.csv*';

COPY INTO STG.SRC_LOANAPPLICATIONDISCOUNTS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanApplicationDiscounts/LoanApplicationDiscounts_Backfill.csv*';

COPY INTO STG.SRC_WEBLOANCREDITFRAUD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($8,'MM/DD/YYYY HH12:MI:SS AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebLoanCreditFraud/WebLoanCreditFraud_Backfill.csv*';

COPY INTO STG.SRC_SECURITYGROUPHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    to_timestamp_ntz($11,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($12,'MM/DD/YYYY HH12:MI:SS AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SecurityGroupHistory/SecurityGroupHistory_Backfill.csv*';

COPY INTO STG.SRC_CH029414LOANBANKCARDUPDATEFROMTRANSLOGSNAPSHOT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0 
    END,
    to_timestamp_ntz($6,'MM/DD/YYYY HH12:MI:SS AM -TZH:TZM'),
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8,
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0 
    END,
    to_timestamp_ntz($11,'MM/DD/YYYY HH12:MI:SS AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CH029414LoanBankCardUpdateFromTransLogSnapshot/CH029414LoanBankCardUpdateFromTransLogSnapshot_Backfill.csv*';

COPY INTO STG.SRC_PRICES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
        $96, 
        $97, 
        $98, 
        $99, 
        $100, 
        $101, 
        $102, 
        $103
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PRICES/PRICES_Backfill.csv*';

COPY INTO STG.SRC_ATTORNEY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
        $1, 
        to_timestamp_ntz($2,'MM/DD/YYYY HH12:MI:SS AM'), 
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
        CASE WHEN $13 = 'True' THEN 1
        ELSE 0 
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
        $25, 
        $26, 
        $27
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Attorney/Attorney_Backfill.csv*';

COPY INTO STG.SRC_PAYROLL1_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
        $1, 
        $2, 
        $3, 
        $4, 
        $5, 
        $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PAYROLL1/PAYROLL1_Backfill.csv*';

COPY INTO STG.SRC__SCHEDULEDPRIMARYCARDMANUALFIXES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
        $1, 
        $2, 
        $3, 
        $4, 
        $5, 
        to_timestamp_ntz($6,'MM/DD/YYYY HH12:MI:SS AM'),
        $7,
        $8,
        $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_ScheduledPrimaryCardManualFixes/_ScheduledPrimaryCardManualFixes_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGBUMPUPINCOMETYPEPRIORITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
        $1, 
        $2, 
        $3, 
        CASE WHEN $4 = 'True' THEN 1
        ELSE 0
        END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigBumpUpIncomeTypePriority/LoanProductConfigBumpUpIncomeTypePriority_Backfill.csv*';

COPY INTO STG.SRC_CARDGOVERNORVALIDATIONLOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3,
    $4, 
    $5,
    $6,
    $7, 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    $9, 
    CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CardGovernorValidationLocation/CardGovernorValidationLocation_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGOPENEND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
        CASE WHEN $15 = '' THEN NULL 
        ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
        END, 
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
        CASE WHEN $34 = '' THEN NULL
        ELSE $34
        END, 
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
pattern= 'inbound/SRC/Backfill/LoanProductConfigOpenEnd/LoanProductConfigOpenEnd_Backfill.csv*';

COPY INTO STG.SRC_SDNALTERNATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    $5

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SDNAlternate/SDNAlternate_Backfill.csv*';

COPY INTO STG.SRC__CH040985_CSV_DATA_FOR_NOOA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'), 
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
    $50

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CH040985_CSV_Data_For_NOOA/L_CH040985_CSV_Data_For_NOOA_Backfill.csv*';

COPY INTO STG.SRC_TRANSACTIONPROCESSORCONFIGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0 
    END, 
    $6, 
    $7, 
    $8,
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0 
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionProcessorConfigHistory/TransactionProcessorConfigHistory_Backfill.csv*';

COPY INTO STG.SRC_NOADVERSEACTIONLETTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM'), 
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM -TZH:TZM')
    END,
    $13,
    $14,
    $15 

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/NoAdverseActionLetter/NoAdverseActionLetter_Backfill.csv*';

COPY INTO STG.SRC_INCOMEVERIFYMETHODLOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/IncomeVerifyMethodLocation/IncomeVerifyMethodLocation_Backfill.csv*';

COPY INTO STG.SRC_EOSCARBATCH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/EOscarBatch/EOscarBatch_Backfill.csv*';

COPY INTO STG.SRC_GLACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/GLAcct/GLAcct_Backfill.csv*';

COPY INTO STG.SRC_DISCOUNTLOCATIONS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DiscountLocations/DiscountLocations_Backfill.csv*';

COPY INTO STG.SRC_SDNMAIN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/SDNMain/SDNMain_Backfill.csv*';

COPY INTO STG.SRC_SDNADDRESS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/SDNAddress/SDNAddress_Backfill.csv*';

COPY INTO STG.SRC_LOANPAYMENTWAIVERIFEE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanPaymentWaiveRIFee/LoanPaymentWaiveRIFee_Backfill.csv*';

COPY INTO STG.SRC_BOOLEANQUESTIONRESPONSEREFINANCELOANAPPLICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BooleanQuestionResponseRefinanceLoanApplication/BooleanQuestionResponseRefinanceLoanApplication_Backfill.csv*';

COPY INTO STG.SRC_PTPPAYMENTPLANLOANPRODUCTENABLENEWLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PTPPaymentPlanLoanProductEnableNewLoan/PTPPaymentPlanLoanProductEnableNewLoan_Backfill.csv*';


COPY INTO STG.SRC_TASKACTIONRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    $13,
    $14,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TaskActionResult/TaskActionResult_Backfill.csv*';

COPY INTO STG.SRC__CH040985_MISSINGEQUIFAXREASONS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    $12,
    $13,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_CH040985_MissingEquifaxReasons/_CH040985_MissingEquifaxReasons_Backfill.csv*';

COPY INTO STG.SRC_LOANDOCHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    END,
    to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanDocHistory/LoanDocHistory_Backfill.csv*';

COPY INTO STG.SRC_INITGLLIST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/InitGLList/InitGLList_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTCONFIGTITLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    $53

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductConfigTitle/LoanProductConfigTitle_Backfill.csv*';


COPY INTO STG.SRC_COMPANY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    CASE WHEN $25 = '' THEN NULL 
    ELSE to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
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
    CASE WHEN $35 = '' THEN NULL 
    ELSE to_timestamp_ntz($35, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
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
    $76, 
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
    CASE WHEN $94 = '' THEN NULL
    ELSE $94
    END, 
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
    CASE WHEN $136 = 'True' THEN 1
    ELSE 0
    END, 
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
    CASE WHEN $163 = 'True' THEN 1
    ELSE 0
    END, 
    $164, 
    $165, 
    $166, 
    $167, 
    $168, 
    $169, 
    $170, 
    $171
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/Company/Company_Backfill.csv*';

COPY INTO STG.SRC__AARCC_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_AARCC/_AARCC_Backfill.csv*';

COPY INTO STG.SRC_EXTERNALAPPRUNDATES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2, 
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExternalAppRunDates/ExternalAppRunDates_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLEMAILTEMPLATESHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    $5, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    $10, 
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallEmailTemplatesHistory/WebCallEmailTemplatesHistory_Backfill.csv*';

COPY INTO STG.SRC_EXTERNALAPPCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ExternalAppConfig/ExternalAppConfig_Backfill.csv*';

COPY INTO STG.SRC_WEBCALLRARR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END, 
    $6, 
    $7, 
    CASE WHEN $8 = 'True' THEN 1
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
    CASE WHEN $14 = 'True' THEN 1
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
    END,
    $20,
    to_timestamp_ntz($21, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'),
    $22,
    $23

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/WebCallRARR/WebCallRARR_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLANREQUESTLOANXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanRequestLoanXRef/PaymentPlanRequestLoanXRef_Backfill.csv*';

COPY INTO STG.SRC_EMAILVERIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'), 
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    $7, 
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
    END, 
    $9

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EmailVerification/EmailVerification_Backfill.csv*';

COPY INTO STG.SRC_LOANPRODUCTTILA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    CASE WHEN $20 = '' THEN NULL 
    ELSE to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $21, 
    $22,
    $23, 
    $24, 
    $25, 
    $26

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanProductTila/LoanProductTila_Backfill.csv*';

COPY INTO STG.SRC_REFINANCELOANAPPLICATIONPENDINGREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/RefinanceLoanApplicationPendingReason/RefinanceLoanApplicationPendingReason_Backfill.csv*';

COPY INTO STG.SRC_TRANSACTIONPROCESSORCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END, 
    $6, 
    $7, 
    $8, 
    $9

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/TransactionProcessorConfig/TransactionProcessorConfig_Backfill.csv*';

COPY INTO STG.SRC_CREDITCARDTRANSREPOSTPAYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM'), 
    $6

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CreditCardTransRepostPayment/CreditCardTransRepostPayment_Backfill.csv*';

COPY INTO STG.SRC_DOCUMENTSPLITHISTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1, 
    $2,
    $3
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DocumentSplitHistoryDetail/DocumentSplitHistoryDetail_Backfill.csv*';

COPY INTO STG.SRC___CARDSTODELETE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/__CardsToDelete/__CardsToDelete_Backfill.csv*';

COPY INTO STG.SRC_LOANCOPLEDGER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
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
    $31
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/LoanCoPledger/LoanCoPledger_Backfill.csv*';

COPY INTO STG.SRC_CUSTOMERMERGEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CustomerMergeHistory/CustomerMergeHistory_Backfill.csv*';

COPY INTO STG.SRC_CFPB_ASSUMEDBADLOANAUTHS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/CFPB_AssumedBadLoanAuths/CFPB_AssumedBadLoanAuths_Backfill.csv*';

COPY INTO STG.SRC_VAULTMGRAUTHORIZATIONNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/VaultMgrAuthorizationNote/VaultMgrAuthorizationNote_Backfill.csv*';.

COPY INTO STG.SRC_CUSTOMERBUSINESS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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

COPY INTO STG.SRC_EDITHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    $5, 
    $6, 
    $7, 
    $8, 
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/EditHistory/EditHistory_Backfill.csv*';

COPY INTO STG.SRC_GOLDTRANSFER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5, 
    $6, 
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
    END,
    $12,
    $13
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/GoldTransfer/GoldTransfer_Backfill.csv*';.

COPY INTO STG.SRC_WEBCALLRARRCONFIGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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

COPY INTO STG.SRC_COLLECTIONAGINGCONFIGDAYSBACKFILL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4,
    $5, 
    $6, 
    $7, 
    $8, 
    $9,
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CollectionAgingConfigDaysBackfill/CollectionAgingConfigDaysBackfill_Backfill.csv*';

COPY INTO STG.SRC_CALLCAMPAIGNDONOTCALL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM TZH:TZM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CallCampaignDoNotCall/CallCampaignDoNotCall_Backfill.csv*';

COPY INTO STG.SRC_CAPSHOLD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
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
    $19,
    $20,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
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
    CASE WHEN $32 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $33 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $34 = 'True' THEN 1
    ELSE 0
    END,
    $35,
    $36,
    $37,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $39 = '' THEN NULL
    ELSE $39
    END,
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END,
    $41,
    CASE WHEN $42 = 'True' THEN 1
    ELSE 0
    END,
    $43,
    $44,
    CASE WHEN $45 = '' THEN NULL 
    ELSE to_timestamp_ntz($45, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $46,
    $47,
    $48,
    $49,
    CASE WHEN $50 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/CapsHold/CapsHold_Backfill.csv*';

COPY INTO STG.SRC_DIALERJOBLOANPRODUCTENABLENEWLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/DialerJobLoanProductEnableNewLoan/DialerJobLoanProductEnableNewLoan_Backfill.csv*';

COPY INTO STG.SRC_MOSTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/MOStatus/MOStatus_Backfill.csv*';

COPY INTO STG.SRC_PAYMENTPLANREQUESTVISITORDOCUMENTXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/PaymentPlanRequestVisitorDocumentXRef/PaymentPlanRequestVisitorDocumentXRef_Backfill.csv*';

COPY INTO STG.SRC_ZIPAREA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/ZipArea/ZipArea_Backfill.csv*';

COPY INTO STG.SRC_LOANBANKCARDPENDING_HOLD_PREV_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
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
pattern= 'inbound/SRC/Backfill/LoanBankCardPending_Hold_Prev/LoanBankCardPending_Hold_Prev_Backfill.csv*';

COPY INTO STG.SRC_OUTOFWALLETERROR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss PM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/OutOfWalletError/OutOfWalletError_Backfill.csv*';


COPY INTO STG.SRC_BUSINESSLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6, 
    $7, 
    $8, 
    CASE WHEN $9 = '' THEN NULL 
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
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
    $35
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/BusinessLoan/BusinessLoan_Backfill.csv*';

COPY INTO STG.SRC__IN600631_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-25'),
    $1
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/SRC/Backfill/_IN600631/_IN600631_Backfill.csv*';

