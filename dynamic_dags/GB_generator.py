from jinja2 import FileSystemLoader, Environment
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))

env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template("GB_template_dag.jinja2")


for filename in os.listdir(file_dir):
    if filename.endswith(".yaml") and filename.startswith("GB"):
        with open(f"{file_dir}/{filename}", "r") as config_file:

            config = yaml.safe_load(config_file)

            with open(f"dags/{config['dag_id']}.py", "w") as f:
                f.write(template.render(config))
