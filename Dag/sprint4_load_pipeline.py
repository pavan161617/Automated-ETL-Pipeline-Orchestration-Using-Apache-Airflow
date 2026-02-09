from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pandas as pd
import os

TRANSFORM_DIR = "/opt/airflow/data/transformed"
FINAL_DIR = "/opt/airflow/data/final"


# -------------------------
# Failure Alert (Email)
# -------------------------
def task_failure_alert(context):
    subject = f"Airflow Alert: {context['task_instance'].task_id} Failed"
    body = f"""
    <h3>Airflow Task Failure</h3>
    <b>DAG:</b> {context['dag'].dag_id}<br>
    <b>Task:</b> {context['task_instance'].task_id}<br>
    <b>Execution Time:</b> {context['execution_date']}<br>
    <b>Log URL:</b> {context['task_instance'].log_url}
    """
    send_email(
        to=["admin@example.com"],
        subject=subject,
        html_content=body
    )


# -------------------------
# Load Transformed Data
# -------------------------
def load_intel_data(ti):
    os.makedirs(FINAL_DIR, exist_ok=True)

    files = sorted(
        [f for f in os.listdir(TRANSFORM_DIR) if f.startswith("intel_daily_summary")]
    )

    if not files:
        raise FileNotFoundError("No transformed Intel files found")

    latest_file = files[-1]
    input_path = f"{TRANSFORM_DIR}/{latest_file}"

    df = pd.read_csv(input_path)

    output_file = f"{FINAL_DIR}/intel_final_{latest_file}"

    # 🔐 IDEMPOTENT LOAD
    if os.path.exists(output_file):
        os.remove(output_file)

    df.to_csv(output_file, index=False)

    ti.xcom_push(key="rows_loaded", value=len(df))
    ti.xcom_push(key="output_file", value=output_file)

    print("Idempotent load completed")


# -------------------------
# Load Summary Logs
# -------------------------
def load_summary(ti):
    rows_loaded = ti.xcom_pull(task_ids="load_intel_data", key="rows_loaded")
    output_file = ti.xcom_pull(task_ids="load_intel_data", key="output_file")

    print("INTEL DATA LOAD SUMMARY")
    print("-----------------------")
    print(f"Rows Loaded : {rows_loaded}")
    print(f"Final File  : {output_file}")


# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="sprint4_load_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=20),
        "sla": timedelta(minutes=5),
        "on_failure_callback": task_failure_alert,
    },
    tags=["sprint4", "intel", "load"],
) as dag:

    start = EmptyOperator(task_id="start")

    load = PythonOperator(
        task_id="load_intel_data",
        python_callable=load_intel_data,
    )

    summary = PythonOperator(
        task_id="load_summary",
        python_callable=load_summary,
    )

    end = EmptyOperator(task_id="end")

    start >> load >> summary >> end
