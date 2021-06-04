from datetime import timedelta, datetime

from typing import Any

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.decorators import dag, task
from airflow.operators.python import task, get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from plugins.smart_file_sensor import SmartFileSensor
from airflow.utils.decorators import apply_defaults

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


SLACK_TOKEN = Variable().get_variable_from_secrets(key="slack_secret") # fetch slack token from Vault
DBS = ["DB_1", "DB_2", "DB_3"]

TRIGGER_DIR = Variable.get("trigger_dir") # fetch local path with run file
# SLACK_TOKEN = Variable.get("slack_token")
DAG_ID = "DAG_SENSOR"
SUB_DAG_ID = "XCOM_sub_dag"
START_DATE = datetime(2000,1,1)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": True,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
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

CONFIGS = {
    DAG_ID: {
        "schedule_interval": None, "start_date": START_DATE
        }
    }

class SmartFileSensor(FileSensor):
    """ custom smart sensor """ 
    poke_context_fields = ('filepath', 'fs_conn_id')

    @apply_defaults
    def __init__(self,  **kwargs: Any):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = not self.soft_fail and super().is_smart_sensor_compatible()
        return result


def sub_dag_processing():
    """ SubDAG: 
    external_sensor (waiting for DB_1 update)  ->
    print_log (pulling xcom with count for rows from DB_1 and print it) ->
    delete_file (run file in TRIGGER_DIR) -> 
    print_status (print task_instance details)
    +
    near example of TaskGroup
    """
    sub_dag = DAG(
        dag_id=f"{DAG_ID}.{SUB_DAG_ID}", 
        default_args=DEFAULT_ARGS,
        schedule_interval=CONFIGS[DAG_ID]["schedule_interval"],
        start_date=CONFIGS[DAG_ID]["start_date"],
        tags=["example"]
    )

    with sub_dag:

        @task()
        def print_logs():
            ti = get_current_context()
            for db in DBS:
                msg = ti["ti"].xcom_pull(
                    task_ids=f"query", dag_id=f"dag_id_{db}",
                    key=f"{db}_rows_count", include_prior_dates=True,
                )
                print(f"the pulled message is: {msg}")


        def create_section():
            """
            Create tasks in the outer section.
            There is broken link in the course, so I copypasted example from gridU
            """
            dummies = [DummyOperator(task_id=f'task-{i + 1}') for i in range(5)]

            with TaskGroup("inside_section_1") as inside_section_1:
                _ = [DummyOperator(task_id=f'task-{i + 1}',) for i in range(3)]

            with TaskGroup("inside_section_2") as inside_section_2:
                _ = [DummyOperator(task_id=f'task-{i + 1}',) for i in range(3)]

            dummies[-1] >> inside_section_1
            dummies[-2] >> inside_section_2

        ext_sensor = ExternalTaskSensor(
            task_id="waiting_for_DB_1_update",
            external_dag_id=DAG_ID,
            external_task_id="trigger_database_update",
            # execution_delta=timedelta(minutes=5)
        )
        
        task_print_logs = print_logs()

        task_remove_file = BashOperator(task_id="delete_run_file", bash_command=f"rm {TRIGGER_DIR}")

        task_finished = BashOperator(task_id="finish_op", bash_command="echo {{ts_nodash}} ")

        ext_sensor >> task_print_logs >> task_remove_file >> task_finished 


        start = DummyOperator(task_id="start")
        with TaskGroup("section_1", tooltip="Tasks for Section 1") as section_1:
            create_section()

        some_other_task = DummyOperator(task_id="some-other-task")
        with TaskGroup("section_2", tooltip="Tasks for Section 2") as section_2:
            create_section()

        end = DummyOperator(task_id='end')
        start >> section_1 >> some_other_task >> section_2 >> end

    return sub_dag


dag = DAG(DAG_ID,
        default_args=DEFAULT_ARGS,
        description=f"DAG_SENSOR",
        schedule_interval=CONFIGS[DAG_ID]["schedule_interval"],
        start_date=CONFIGS[DAG_ID]["start_date"],
        tags=["example"],
    )

with dag:
    """ main DAG:
    smart_sensor (looking for run file) ->
    trigger_external_dag (dag_id_DB_1) -> 
    SubDAG (external_sensor -> print_logs -> remove_file -> print_finish_log | example TaskGroup) -> 
    send_message (into Slack chanell)
    """
    @task()
    def slack_send_message():
        client = WebClient(token=SLACK_TOKEN)
        try:
            response = client.chat_postMessage(channel="airflowtask33", text="Hello from your app! :tada:")
        except SlackApiError as e:
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'


    sens = SmartFileSensor(task_id="checking_file", filepath=TRIGGER_DIR, fs_conn_id='fs_default')

    task_trigger = TriggerDagRunOperator(
        task_id="trigger_database_update", trigger_dag_id="dag_id_DB_1", wait_for_completion=True, poke_interval=15,
    )

    sub_dag = SubDagOperator(task_id='XCOM_sub_dag', subdag = sub_dag_processing(), default_args=DEFAULT_ARGS)

    task_slack = slack_send_message()

    sens >> task_trigger >> sub_dag >> task_slack