from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(f"{__file__}/../"))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('templates/staging_dag_template.jinja2')

for filename in os.listdir(f"{file_dir}/inputs/staging"):
    print(filename)
    if filename.endswith('.yaml'):
        with open(f"{file_dir}/inputs/staging/{filename}","r") as input_file:
            inputs = yaml.safe_load(input_file)
            with open(f"dags/staging/{inputs['dag_id']}_temp.py", "w") as f:
                f.write(template.render(inputs))
