from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import time


def slow_task():
    time.sleep(30)
    print("Task completed")


def sla_miss_callback(dag, task_list, blocking_task_list, slas, **kwargs):
    print("🚨 SLA MISS DETECTED 🚨")
    print(f"DAG: {dag.dag_id}")
    print(f"Tasks: {task_list}")
    print(f"Blocking tasks: {blocking_task_list}")


with DAG(
    dag_id="sprint6_sla_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "sla": timedelta(seconds=10)
    },
    sla_miss_callback=sla_miss_callback,
    tags=["sprint6", "sla"],
) as dag:

    start = EmptyOperator(task_id="start")

    slow = PythonOperator(
        task_id="slow_task",
        python_callable=slow_task,
    )

    end = EmptyOperator(task_id="end")

    start >> slow >> end
