from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(f"{__file__}/../"))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('templates/generate_ddl_dag_template.jinja2')

for filename in os.listdir(f"{file_dir}/inputs/generate_ddl"):
    print(filename)
    if filename.endswith('.yaml'):
        with open(f"{file_dir}/inputs/generate_ddl/{filename}","r") as input_file:
            inputs = yaml.safe_load(input_file)
            with open(f"dags/generate_ddl/{inputs['dag_id']}_temp.py", "w") as f:
                f.write(template.render(inputs))
