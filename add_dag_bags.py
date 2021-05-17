
""" add additional DAGs folders """
from airflow.models import DagBag

dags_dirs = ['path/to/dag_dir_1', 'path/to/dag_dir_2', 'path/to/dag_dir_3']

for dir in dags_dirs:
 dag_bag = DagBag(dir)

 if dag_bag:
    for dag_id, dag in dag_bag.dags.items():
        globals()[dag_id] = dag