
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

DBS = ["DB-1", "DB-2", "DB-3"]
config = {
    f"dag_id_{db_name}": {
        "schedule_interval": None, "start_date": datetime(2021, 5, 11), "database": db_name
        } for db_name in DBS}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
    # "wait_for_downstream": False,
    # "dag": dag,
    # "sla": timedelta(hours=2),
    # "execution_timeout": timedelta(seconds=300),
    # "on_failure_callback": some_function,
    # "on_success_callback": some_other_function,
    # "on_retry_callback": another_function,
    # "sla_miss_callback": yet_another_function,
    # "trigger_rule": "all_success"
}

def print_logs(dag_id, database):
    """Print information about processing steps"""
    print(f"{dag_id} start processing tables in database: {database}")


for dag_id in config: 
    with DAG(dag_id,
        default_args=default_args,
        description=f"DAG in the loop {dag_id}",
        schedule_interval=config[dag_id]["schedule_interval"],
        start_date=config[dag_id]["start_date"],
        tags=["example"],
    ) as dag:

        task_logs = PythonOperator(
        task_id=f"print_logs_{dag_id}",
        python_callable=print_logs,
        op_kwargs={"dag_id": dag_id, "database": config[dag_id]["database"]},
        )

        task_ins = DummyOperator(task_id="insert_new_row")

        task_query = DummyOperator(task_id="query_the_table")

        task_logs >> task_ins >> task_query