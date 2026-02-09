from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from intel_plugin.hooks.intel_file_hook import IntelFileHook


def test_hook():
    hook = IntelFileHook()
    exists = hook.file_exists("preprocessed/preprocessed_data.csv")
    size = hook.get_file_size("preprocessed/preprocessed_data.csv")
    print(f"Exists: {exists}, Size: {size}")


with DAG(
    dag_id="test_intel_file_hook",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint5", "hook"],
) as dag:

    test = PythonOperator(
        task_id="test_hook",
        python_callable=test_hook,
    )
