from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import os

CLEAN_FILE = "/opt/airflow/data/cleaned/intel_cleaned.csv"


def profile_data(ti):
    if not os.path.exists(CLEAN_FILE):
        raise FileNotFoundError("Cleaned Intel dataset not found")

    df = pd.read_csv(CLEAN_FILE)

    profile = {
        "total_rows": len(df),
        "null_counts": df.isnull().sum().to_dict(),
        "temperature_min": df["temperature"].min(),
        "temperature_max": df["temperature"].max(),
        "temperature_avg": df["temperature"].mean(),
        "humidity_min": df["humidity"].min(),
        "humidity_max": df["humidity"].max(),
        "humidity_avg": df["humidity"].mean(),
        "voltage_min": df["voltage"].min(),
        "voltage_max": df["voltage"].max(),
    }

    # Push profile stats to XCom
    ti.xcom_push(key="profile_stats", value=profile)

    print("DATA PROFILING SUMMARY")
    print("----------------------")
    for k, v in profile.items():
        print(f"{k}: {v}")


with DAG(
    dag_id="sprint2_data_profiling",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["profiling", "intel", "quality"],
) as dag:

    start = EmptyOperator(task_id="start")

    profile = PythonOperator(
        task_id="profile_data",
        python_callable=profile_data,
    )

    end = EmptyOperator(task_id="end")

    start >> profile >> end
