SELECT 
LAST_DAY(to_Date(('{{ ti.xcom_pull(task_ids= 'LD.get_run_param', key='rptEndDate') }}')  ))  as RptEndDate
, current_date() as dss_load_time					
, '{{ params.file_format }}' file_format
, '{{ ti.xcom_pull(task_ids= 'LD.get_run_param', key='pattern') }}'   pattern   
FROM  {{ params.table_name }}