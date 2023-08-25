COPY INTO STG.LD_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-03-03'),
    $1,
    $2,
    $3
from @ETL.INBOUND t
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/BalSheet_TransDetail/LD_BALSHEET_TRANSDETAIL_MAX.csv.gz.*';

COPY INTO STG.LD_LOANPAYMENTOPENEND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-03-03'),
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
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
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
from @ETL.INBOUND t
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/LoanPaymentOpenEnd/LD_LOANPAYMENTOPENEND_MAX.csv.gz.*';

COPY INTO STG.LD_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-03-03'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
from @ETL.INBOUND t
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/TRANSDETAILACCT/LD_TRANSDETAILACCT_MAX.csv.gz.*';

COPY INTO STG.LD_ENDOFDAYINVENTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-03-03'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
from @ETL.INBOUND t
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/EndOfDayInventoryDetail/LD_EndOfDayInventoryDetail_MAX.csv.gz.*';