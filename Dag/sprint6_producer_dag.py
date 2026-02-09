from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="sprint6_producer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint6", "producer"],
) as dag:

    start = EmptyOperator(task_id="start")

    process = EmptyOperator(task_id="process_data")

    end = EmptyOperator(task_id="end")

    start >> process >> end
