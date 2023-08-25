-- loading new S3 files into the bank table
insert into STG.SmartTreasury_Bank_HIST (
    select METADATA$FILENAME as FILENAME
    , METADATA$FILE_LAST_MODIFIED as FILE_LAST_MODIFIED
    , to_date(current_timestamp()) as DATE_LOADED
    , METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER
    , etl.$1
    , etl.$2
    , etl.$3
    , etl.$4
    , etl.$5
    , etl.$6
    , etl.$7
    , etl.$8
    , etl.$9
    , etl.$10 
    from @ETL.INBOUND/SmartTreasury etl
    where filename not in (
        select filename from stg.smarttreasury_bank_hist
    )
);

-- loading historical data in transaction details
insert into FIN.SMARTTREASURY_TRANSACTIONDETAILS_HIST (
    with acct_num as (
        select file_row_number + 1 as File_Row_num_H, 
        LEAD(FILE_ROW_NUMBER) OVER (PARTITION BY FILENAME, COLUMN1 ORDER BY FILE_ROW_NUMBER) - 1 AS File_Row_num_F,
        LEAD(FILE_ROW_NUMBER) OVER (PARTITION BY FILENAME, COLUMN1 ORDER BY FILE_ROW_NUMBER) AS File_Row_num_L, 
        *
        from stg.smarttreasury_bank_hist 
        where column1 = 03 
        order by filename, file_row_number
    )
    , orig_date as (
        select to_date(COLUMN5, 'YYMMDD') as date, 
        filename 
        from stg.smarttreasury_bank_hist 
        where COLUMN1 = 02 
        order by filename, file_row_number
    )
    SELECT distinct 
    bh.FILENAME,
    bh.LOADTIMESTAMP,
    bh.ASOFDATE,
    bh.FILE_ROW_NUMBER,
    o.date as "Origination Date",
    ad.column2 cust_acc_number,
    bh.COLUMN1,
    bh.COLUMN2,
    bh.COLUMN3,
    bh.COLUMN4,
    (bh.COLUMN5 / 100) as AMOUNT,
    bh.COLUMN6,
    bh.COLUMN7,
    bh.COLUMN8,
    bh.COLUMN9
    FROM stg.smarttreasury_bank_hist bh
    Left join acct_num ad on ad.filename = bh.filename
    Left join orig_date o on o.filename = ad.filename
    where bh.FILE_ROW_NUMBER between ad.File_Row_num_H and ad.File_Row_num_F
    and bh.column1 = 16
    and bh.filename in (select filename from stg.smarttreasury_bank_hist)
    order by bh.filename, bh.file_row_number
);

--loading historical data into account header table
insert into FIN.smarttreasury_accountheaderdetails_hist (
    select * from stg.smarttreasury_bank_hist 
    where COLUMN1 = 03 
    and filename in (
        select filename 
        from stg.smarttreasury_bank_hist
    ) 
    order by file_row_number
);
