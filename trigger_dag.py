from datetime import timedelta, datetime

from airflow import DAG
from airflow.models.base import Base
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

DBS = ["DB_1", "DB_2", "DB_3"]

TRIGGER_DIR = Variable.get("trigger_dir")
SLACK_TOKEN = Variable.get("slack_token")
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


def sub_dag_processing():
        
    sub_dag = DAG(
        dag_id=f"{DAG_ID}.{SUB_DAG_ID}", 
        default_args=DEFAULT_ARGS,
        schedule_interval=CONFIGS[DAG_ID]["schedule_interval"],
        start_date=CONFIGS[DAG_ID]["start_date"],
        tags=["example"]
    )


    with sub_dag:

        def print_logs(ti):

            for db in DBS:
                msg = ti.xcom_pull(
                    task_ids=f"query", dag_id=f"dag_id_{db}",
                    key=f"{db}_rows_count", include_prior_dates=True,
                )
                print(f"the pulled message is: {msg}")


        ext_sensor = ExternalTaskSensor(
            task_id="waiting_for_trigger_another_tasK_acomplish",
            external_dag_id=DAG_ID,
            external_task_id="trigger_database_update",
            # execution_delta=timedelta(minutes=5)
        )

        task_print_res = PythonOperator(
            task_id="print_result",
            python_callable=print_logs
            )

        task_remove_file = BashOperator(task_id="delete_run_file", bash_command=f"rm {TRIGGER_DIR}")

        task_finished = BashOperator(task_id="finish_op", bash_command="echo {{ts_nodash}} ")

        ext_sensor >> task_print_res >> task_remove_file >> task_finished 

    return sub_dag

print(TRIGGER_DIR)
dag = DAG(DAG_ID,
        default_args=DEFAULT_ARGS,
        description=f"DAG_SENSOR",
        schedule_interval=CONFIGS[DAG_ID]["schedule_interval"],
        start_date=CONFIGS[DAG_ID]["start_date"],
        tags=["example"],
    )

with dag:

    def slack_send_message():

        client = WebClient(token=SLACK_TOKEN)
        try:
            response = client.chat_postMessage(channel="airflowtask33", text="Hello from your app! :tada:")
        except SlackApiError as e:
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'


    sens = FileSensor(
        task_id="checking_file", filepath=TRIGGER_DIR, fs_conn_id="check_file_conn", poke_interval=15
    )

    task_trigger = TriggerDagRunOperator(
        task_id="trigger_database_update", trigger_dag_id="dag_id_DB_1", wait_for_completion=True, poke_interval=15,
    )

    sub_dag = SubDagOperator(
        task_id='XCOM_sub_dag',
        subdag = sub_dag_processing(),
        default_args=DEFAULT_ARGS,
    )

    task_slack = PythonOperator(task_id="send_message_in_slack", python_callable=slack_send_message)

    sens >> task_trigger >> sub_dag >> task_slack

