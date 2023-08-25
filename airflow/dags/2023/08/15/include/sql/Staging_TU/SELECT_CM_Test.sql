SELECT 
LAST_DAY(to_Date(('{{ ti.xcom_pull(task_ids= 'CM.get_run_param', key='rptEndDate') }}')  ))  as RptEndDate
--, LAST_DAY(to_Date(('{{ params.RptEndDate }}')  ))  as RptEndDate2
, current_date() as dss_load_time					
, '{{ params.file_format }}' file_format
, '{{ ti.xcom_pull(task_ids= 'CM.get_run_param', key='pattern') }}'   pattern   
--, '{{ params.pattern }}'   pattern2
FROM  {{ params.table_name }}