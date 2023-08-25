from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(f"{__file__}/../"))
env = Environment(loader=FileSystemLoader(file_dir))

template = env.get_template('templates/backfill_dag_mssql_template.jinja2')
for filename in os.listdir(f"{file_dir}/inputs/backfill/mssql"):
    print(filename)
    if filename.endswith('.yaml'):
        with open(f"{file_dir}/inputs/backfill/mssql/{filename}","r") as input_file:
            inputs = yaml.safe_load(input_file)
            with open(f"dags/backfill/{inputs['dag_id']}_temp.py", "w") as f:
                f.write(template.render(inputs))

template = env.get_template('templates/backfill_dag_pandas_template.jinja2')
for filename in os.listdir(f"{file_dir}/inputs/backfill/pd"):
    print(filename)
    if filename.endswith('.yaml'):
        with open(f"{file_dir}/inputs/backfill/pd/{filename}","r") as input_file:
            inputs = yaml.safe_load(input_file)
            with open(f"dags/backfill/{inputs['dag_id']}_temp.py", "w") as f:
                f.write(template.render(inputs))
