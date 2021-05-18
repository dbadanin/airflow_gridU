
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

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
def create_dag(dag_id,
               schedule_unterval_custom,
               start_date_custom,
               database_name,
               default_args):
    
    def print_process_start(dag_id, database):
        """Print information about processing steps"""
        print(f"{dag_id} start processing tables in database: {database}")

    def check_table_exists(cond):
        if cond == True:
            return "insert_row"
        else:
            return "create_table"

    dag = DAG(dag_id,
        default_args=default_args,
        description=f"DAG in the loop {dag_id}",
        schedule_interval=schedule_unterval_custom,
        start_date=start_date_custom,
        tags=["example"],
    )
    with dag:
        
        task_logs = PythonOperator(
        task_id=f"print_logs_{dag_id}",
        python_callable=print_process_start,
        op_kwargs={"dag_id": dag_id, "database": database_name},
        )

        task_check_table = BranchPythonOperator(
            task_id="check_table_exist",
            python_callable=check_table_exists,
            dag=dag)

        task_create_table = DummyOperator(task_id=f"create_table")

        task_ins = DummyOperator(task_id=f"insert_row")

        task_query = DummyOperator(task_id=f"query_the_table")

        task_logs >> task_check_table >> [task_create_table, task_ins] >> task_query

    return dag

for dag_id in config: 
    globals()[dag_id] = create_dag(dag_id,
                                  config[dag_id]["schedule_interval"],
                                  config[dag_id]["start_date"],
                                  config[dag_id]["database"],
                                  default_args)
         
         