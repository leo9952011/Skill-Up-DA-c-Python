from jinja2 import FileSystemLoader, Environment
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))

print(file_dir)

# LISTA DE GRUPOS
for grupo in ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]:

    env = Environment(loader=FileSystemLoader(file_dir))
    try:
        template = env.get_template(f"G{grupo}_template_dag.jinja2")
    except:
        print(f"No existe un template para el grupo: {grupo}")

    for filename in os.listdir(file_dir):
        if filename.endswith(".yaml") and filename.startswith(f"G{grupo}"):
            with open(f"{file_dir}/{filename}", "r") as config_file:

                config = yaml.safe_load(config_file)

                with open(f"dags/{config['dag_id']}_dag_etl.py", "w") as f:
                    f.write(template.render(config))
