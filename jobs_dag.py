
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import xcom
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import schema
from random import randint


from airflow.models.baseoperator import BaseOperator


class PostgresGetCountRows(BaseOperator):   

        def __init__(
                self,
                table_name: str,
                post_conn_id: str,
                **kwargs) -> None:
            super().__init__(**kwargs)
            self.table_name = table_name
            self.post_conn_id = post_conn_id

        def execute(self, context):
            hook = PostgresHook(postgres_conn_id=self.post_conn_id)
            connection = hook.get_conn()
            cursor = connection.cursor()    
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name.lower()};")
            count_r = cursor.fetchall()
            context["ti"].xcom_push(key=f"{self.table_name}_rows_count", value=count_r)
            # sql = "select name from user"
            # result = hook.get_first(sql)
            # message = "Hello {}".format(result['name'])
            # print(message)
            # return message


DBS = ["DB_1", "DB_2", "DB_3"]
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
        inf = f"{dag_id} start processing tables in database: {database}"
        print(inf)


    def check_table_exist(sql_to_get_schema, sql_to_check_table_exist, table_name):
        """ callable function to get schema name and after that check if table exist """ 
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        
        query = hook.get_first(sql=sql_to_check_table_exist.format(table_name.lower()))
    
        if query:
            return "insert_row"
        else:
            return "create_table"
    
    def insert_row(sql_query, table_name, custom_id, dt_now, **kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        connection = hook.get_conn()
        cursor = connection.cursor()
    
        cursor.execute(
            sql_query, (custom_id, kwargs["ti"].xcom_pull(task_ids="getting_current_user"), dt_now)
        )
        connection.commit()

    def rows_count(sql_query, table_name, **kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        connection = hook.get_conn()
        cursor = connection.cursor()    
        cursor.execute(sql_query)
        count_r = cursor.fetchall()
        print(count_r)
        kwargs["ti"].xcom_push(key=f"{table_name}_rows_count", value=count_r)
    
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

        task_bash = BashOperator(task_id="getting_current_user", bash_command="whoami")

        task_check_table = BranchPythonOperator(
            task_id="check_table_exist", python_callable=check_table_exist,
            op_args=["SELECT * FROM pg_tables;",
                    "SELECT * FROM information_schema.tables "
                    "WHERE table_name = '{}';", database_name], dag=dag)

        task_create_table = PostgresOperator(
            postgres_conn_id="postgres_conn",
            task_id='create_table',
            sql=f'''CREATE TABLE {database_name} (
                custom_id integer NOT NULL, 
                user_name VARCHAR (50) NOT NULL, 
                timestamp TIMESTAMP NOT NULL
                );''', trigger_rule=TriggerRule.NONE_FAILED,
        )

        task_insert_row = PythonOperator(
            task_id='insert_row',
            python_callable=insert_row,
            op_args=[f"INSERT INTO {database_name.lower()} " 
                "VALUES(%s, %s, %s);", database_name.lower(), randint(1,10), datetime.now()]
        )

        task_query = PostgresGetCountRows(
            database_name, "postgres_conn", task_id="query", trigger_rule=TriggerRule.NONE_FAILED
        )

        task_logs >> task_bash >> task_check_table >> [task_create_table, task_insert_row] >> task_query

    return dag

for dag_id in config: 
    globals()[dag_id] = create_dag(dag_id,
                                  config[dag_id]["schedule_interval"],
                                  config[dag_id]["start_date"],
                                  config[dag_id]["database"],
                                  default_args)
         
         