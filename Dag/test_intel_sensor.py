from airflow import DAG
from datetime import datetime
from intel_plugin.sensors.intel_file_sensor import IntelFileSensor

with DAG(
    dag_id="test_intel_file_sensor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint5", "sensor"],
) as dag:

    wait_for_file = IntelFileSensor(
        task_id="wait_for_preprocessed_file",
        relative_path="preprocessed/preprocessed_data.csv",
        poke_interval=15,
        timeout=120,
        mode="poke",
    )
