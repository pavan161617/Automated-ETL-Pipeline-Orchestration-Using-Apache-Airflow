from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def slow_task(task_id):
    print(f"Running {task_id}")
    time.sleep(20)


with DAG(
    dag_id="sprint6_concurrency_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    concurrency=2,          # Max 2 tasks at a time
    max_active_runs=1,      # Only 1 DAG run at a time
    tags=["sprint6", "concurrency"],
) as dag:

    tasks = []

    for i in range(5):
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=slow_task,
            op_args=[f"task_{i}"],
        )
        tasks.append(task)

    tasks
