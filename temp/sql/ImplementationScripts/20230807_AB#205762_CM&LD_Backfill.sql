COPY INTO STG.CM_SERVICETRANS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-07'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else $5 end, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $11 end, $12, 
case when $13 = 'True' then 1 else 0 end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, $16, $17
from @ETL.inbound/CM/Backfill/CM_ServiceTrans/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/CM_ServiceTrans/CM_ServiceTrans_Backfill_.*';


COPY INTO STG.CM_SERVICEDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-07'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5, $6, $7, $8, 
case when $9 = '' then null else $9 end, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $10 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, $15, $16, 
case when $17 = 'True' then 1 else 0 end
from @ETL.inbound/CM/Backfill/CM_ServiceDetail/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/CM_ServiceDetail/CM_ServiceDetail_Backfill.csv.gz';


COPY INTO STG.CM_CashedCheck_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-07'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, $8, $9, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, $12, $13, 
case when $14 = 'True' then 1 else 0 end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $16 = 'True' then 1 else 0 end, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = '' then null else $18 end, 
case when $19 = 'True' then 1 else 0 end, 
case when $20 = '' then null else $20 end, $21, $22, $23, $24, 
case when $25 = 'True' then 1 else 0 end, $26, $27, 
case when $28 = '' then null else $28 end, 
case when $29 = 'True' then 1 else 0 end, 
case when $30 = 'True' then 1 else 0 end, $31
from @ETL.inbound/CM/Backfill/CM_CashedCheck/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/CM/Backfill/CM_CashedCheck/CM_CashedCheck_Backfill_.*';


COPY INTO STG.LD_SERVICETRANS_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-07'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, 
case when $5 = '' then null else $5 end, $6, $7, $8, $9, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $11 end, $12, 
case when $13 = 'True' then 1 when $13 = 'False' then 0 end, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $16 = 'True' then 1 else 0 end, $17
from @ETL.inbound/LD/Backfill/LD_ServiceTrans/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/LD_ServiceTrans/LD_ServiceTrans_Backfill.csv'
;


COPY INTO STG.LD_SERVICEDETAIL_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-07'), 
$1, $2, $3, 
case when $4 = 'True' then 1 else 0 end, $5, $6, $7, $8, 
case when $9 = '' then null else $9 end, 
case when $10 = '' then null else $10 end, 
case when $11 = '' then null else $10 end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end, $13, 
case when $14 = '' then null else to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:SS AM') end, $15, $16, 
case when $17 = 'True' then 1 else 0 end
from @ETL.inbound/LD/Backfill/LD_ServiceDetail/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/LD_ServiceDetail/LD_ServiceDetail_Backfill.csv';



COPY INTO STG.LD_CashedCheck_HIST FROM
(select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-08-07'), 
$1, $2, $3, 
case when $4 = '' then null else $4 end, $5, 
case when $6 = '' then null else to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:SS AM') end, $7, $8, $9, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, $12, $13,
case when $14 = 'True' then 1 else 0 end, 
case when $15 = '' then null else to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $16 = 'True' then 1 else 0 end, 
case when $17 = 'True' then 1 else 0 end, 
case when $18 = '' then null else $18 end, 
case when $19 = 'True' then 1 else 0 end, 
case when $20 = '' then null else $20 end, $21, $22, $23, $24, 
case when $25 = 'True' then 1 else 0 end, $26, $27, 
case when $28 = '' then null else $28 end, 
case when $29 = 'True' then 1 else 0 end, 
case when $30 = 'True' then 1 else 0 end, $31
from @ETL.inbound/LD/Backfill/LD_CashedCheck/)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/LD/Backfill/LD_CashedCheck/LD_CashedCheck_Backfill.csv';