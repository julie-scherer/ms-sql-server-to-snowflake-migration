COPY INTO STG.VC_GLOBAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-05-03'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL 
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $11,
    $12,
    $13,
    $14,
    CASE WHEN $15 = ''  THEN NULL 
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
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    $46,
    $47,
    CASE WHEN $48 = ''  THEN NULL 
    ELSE to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
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
    CASE WHEN $70 = '' THEN NULL
    ELSE $70
    END,
    CASE WHEN $71 = '' THEN NULL
    ELSE $71
    END,
    CASE WHEN $72 = '' THEN NULL
    ELSE $72
    END,
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
    $101
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Global/Global_Backfill.csv*';