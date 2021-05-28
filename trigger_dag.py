from datetime import timedelta, datetime
import logging
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.models.connection import Connection
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator


Variable.set("trigger_dir", "/opt/airflow/trigger_dir/")
print(Variable.get("trigger_dir"))
trigger_dir = Variable.get("trigger_dir")
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

DAG_ID = "DAG_SENSOR"
SUB_DAG_ID = "XCOM_sub_dag"
config = {
    DAG_ID: {
        "schedule_interval": None, "start_date": datetime(2021, 5, 11)
        }
    }
def pushers_sub_dag(parent_dag_name, child_dag_name, config, default_args):

    def print_result(ti):
        DBS = ["DB-1", "DB-2", "DB-3"]
        for db in DBS:
            msg = ti.xcom_pull(task_ids=f"query_the_table_{db}", dag_id=f"{parent_dag_name}.{child_dag_name}")
            print(f"the pulled message is: {msg}")
            
    sub_dag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}", 
        default_args=default_args,
        schedule_interval=config[DAG_ID]["schedule_interval"],
        start_date=config[DAG_ID]["start_date"],
        tags=["example"]
    )
    with sub_dag:
        ext_sensor = ExternalTaskSensor(
            task_id="waiting_for_main_DAG",
            external_dag_id=parent_dag_name,
            # external_task_id=None,
            # execution_delta=timedelta(minutes=-1)
            
        )

        task_print_res = PythonOperator(
            task_id="print_result",
            python_callable=print_result
            )

        task_remove_file = BashOperator(task_id="delete_run_file", bash_command=f"rm {trigger_dir}")

        task_finished = BashOperator(task_id="finish_op", bash_command="echo {{ts_nodash}} ")

        ext_sensor >> task_print_res >> task_remove_file >> task_finished 

    return sub_dag


dag = DAG(DAG_ID,
        default_args=default_args,
        description=f"DAG_SENSOR",
        schedule_interval=config[DAG_ID]["schedule_interval"],
        start_date=config[DAG_ID]["start_date"],
        tags=["example"],
    )

with dag:

    sens = FileSensor(
        task_id="checking_file", filepath=trigger_dir, fs_conn_id="check_file_conn", poke_interval=5
    )

    task_trigger = TriggerDagRunOperator(
        task_id="trigger_database_update", trigger_dag_id="dag_id_DB-1", wait_for_completion=True, poke_interval=5
    )

    sub_dag = SubDagOperator(
        task_id='XCOM_sub_dag',
        subdag = pushers_sub_dag(DAG_ID, SUB_DAG_ID, config, default_args),
        default_args=default_args,
        dag=dag,
    )
    sens >> task_trigger >> sub_dag

