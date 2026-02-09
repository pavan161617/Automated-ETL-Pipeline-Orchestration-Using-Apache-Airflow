from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="sprint1_hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="hello_task",
        bash_command="echo 'Hello Airflow Sprint 1'"
    )
