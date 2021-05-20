from datetime import timedelta, datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.models.connection import Connection

# conn = Connection(
#     conn_id='check_file_conn',
#     conn_type='fs',
#     description=None,
#     login='airflow',
#     password='airflow',
#     host='http://localhost',
#     port=8080,
#     schema=None,
#     extra=None)
trigger_dir = "/opt/airflow/trigger_dir/1.txt"


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

dag_id = "DAG_SENSOR"
config = {
    dag_id: {
        "schedule_interval": None, "start_date": datetime(2021, 5, 11)
        }
    }

dag = DAG(dag_id,
        default_args=default_args,
        description=f"DAG_SENSOR",
        schedule_interval=config[dag_id]["schedule_interval"],
        start_date=config[dag_id]["start_date"],
        tags=["example"],
    )

with dag:

    sens = FileSensor(
        task_id="Checking_file", filepath=trigger_dir, fs_conn_id="check_file_conn", poke_interval=5
    )

    task_trigger = TriggerDagRunOperator(
        task_id="Trigger_delete_file", trigger_dag_id="dag_id_DB-1",
    )

    task_bash = BashOperator(task_id="Delete_run_file", bash_command=f"rm {trigger_dir}")

    sens >> task_trigger >> task_bash

