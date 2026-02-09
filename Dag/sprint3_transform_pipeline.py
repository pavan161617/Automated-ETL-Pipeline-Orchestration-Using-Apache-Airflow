from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os

CLEAN_FILE = "/opt/airflow/data/cleaned/intel_cleaned.csv"
TRANSFORM_DIR = "/opt/airflow/data/transformed"


def transform_intel_data(ti, execution_date):
    if not os.path.exists(CLEAN_FILE):
        raise FileNotFoundError("Cleaned Intel data not found")

    os.makedirs(TRANSFORM_DIR, exist_ok=True)

    df = pd.read_csv(CLEAN_FILE)

    df["timestamp"] = pd.to_datetime(
        df["date"] + " " + df["time"],
        errors="coerce"
    )

    agg_df = (
        df
        .groupby([df["timestamp"].dt.date, "nodeid"])
        .agg(
            avg_temperature=("temperature", "mean"),
            max_temperature=("temperature", "max"),
            min_temperature=("temperature", "min"),
            avg_humidity=("humidity", "mean"),
            avg_voltage=("voltage", "mean"),
            records=("temperature", "count")
        )
        .reset_index()
        .rename(columns={"timestamp": "date"})
    )

    # 🔐 IDEMPOTENT PARTITION
    partition_date = execution_date.strftime("%Y-%m-%d")
    output_file = f"{TRANSFORM_DIR}/intel_daily_summary_{partition_date}.csv"

    # SAFE OVERWRITE
    if os.path.exists(output_file):
        os.remove(output_file)

    agg_df.to_csv(output_file, index=False)

    ti.xcom_push(key="records_transformed", value=len(agg_df))
    ti.xcom_push(key="output_file", value=output_file)

    print(f"Idempotent transform complete for {partition_date}")


def log_transform_stats(ti):
    records = ti.xcom_pull(
        task_ids="transform_intel_data", key="records_transformed"
    )
    output_file = ti.xcom_pull(
        task_ids="transform_intel_data", key="output_file"
    )

    print("TRANSFORMATION SUMMARY")
    print("----------------------")
    print(f"Records Transformed: {records}")
    print(f"Output File: {output_file}")


with DAG(
    dag_id="sprint3_transform_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
        "sla": timedelta(minutes=3),
    },
    tags=["sprint3", "intel", "transform"],
) as dag:

    start = EmptyOperator(task_id="start")

    transform = PythonOperator(
        task_id="transform_intel_data",
        python_callable=transform_intel_data,
    )

    log = PythonOperator(
        task_id="log_transform_stats",
        python_callable=log_transform_stats,
    )

    end = EmptyOperator(task_id="end")

    start >> transform >> log >> end
