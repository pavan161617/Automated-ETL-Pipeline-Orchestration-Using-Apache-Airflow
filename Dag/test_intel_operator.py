from airflow import DAG
from datetime import datetime
from intel_plugin.operators.intel_file_check_operator import IntelFileCheckOperator


with DAG(
    dag_id="test_intel_file_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint5", "operator"],
) as dag:

    check_file = IntelFileCheckOperator(
        task_id="check_preprocessed_file",
        relative_path="preprocessed/preprocessed_data.csv",
    )
