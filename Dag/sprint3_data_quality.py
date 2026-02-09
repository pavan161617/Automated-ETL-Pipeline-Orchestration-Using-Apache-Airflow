from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import os
import requests

TRANSFORM_DIR = "/opt/airflow/data/transformed"

# 🔔 SLACK WEBHOOK URL
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"


# -------------------------
# Slack Failure Alert
# -------------------------
def slack_failure_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    message = {
        "text": (
            ":rotating_light: *Airflow Task Failed*\n"
            f"*DAG:* {dag_id}\n"
            f"*Task:* {task_id}\n"
            f"*Execution Time:* {exec_date}\n"
            f"*Log:* {log_url}"
        )
    }

    requests.post(SLACK_WEBHOOK_URL, json=message)


# -------------------------
# Data Quality Checks
# -------------------------
def data_quality_checks():
    files = sorted(
        [f for f in os.listdir(TRANSFORM_DIR) if f.startswith("intel_daily_summary")]
    )

    if not files:
        raise FileNotFoundError("No transformed Intel files found")

    latest_file = files[-1]
    file_path = f"{TRANSFORM_DIR}/{latest_file}"

    df = pd.read_csv(file_path)

    # 1. Row count
    if len(df) == 0:
        raise ValueError("No records found")

    # 2. Null checks
    critical_columns = [
        "avg_temperature",
        "avg_humidity",
        "avg_voltage",
        "records",
    ]

    for col in critical_columns:
        if df[col].isnull().any():
            raise ValueError(f"Null values found in {col}")

    # 3. Range checks
    if not df["avg_temperature"].between(-10, 60).all():
        raise ValueError("Temperature out of range")

    if not df["avg_humidity"].between(0, 100).all():
        raise ValueError("Humidity out of range")

    if not (df["records"] > 0).all():
        raise ValueError("Invalid record counts")

    print("DATA QUALITY CHECK PASSED")
    print(f"File Checked: {file_path}")
    print(f"Total Rows: {len(df)}")


# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="sprint3_data_quality",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "on_failure_callback": slack_failure_alert,
    },
    tags=["quality", "intel", "slack"],
) as dag:

    start = EmptyOperator(task_id="start")

    quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
    )

    end = EmptyOperator(task_id="end")

    start >> quality >> end
