COPY INTO DW.ALL_PERIOD_DIM FROM
(select $1, $2, $3, $4, $5, $6, $7, 
case when $8 = '' then null else to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $9 = '' then null else to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:SS AM') end, $10, 
case when $11 = '' then null else to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:SS AM') end, 
case when $12 = '' then null else to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:SS AM') end
from @ETL.inbound/CURODW/DW/All_Period_DIM_Backfill.csv)
FILE_FORMAT = (format_name = ARES.STG.LD_CSV_PIPE_SH1_EON_GZ);