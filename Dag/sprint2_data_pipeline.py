from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import os

# -------------------------
# File paths
# -------------------------
RAW_FILE = "/opt/airflow/data/raw/intel/data.txt"

PREPROCESS_DIR = "/opt/airflow/data/preprocessed"
PREPROCESS_FILE = f"{PREPROCESS_DIR}/preprocessed_data.csv"

CLEAN_DIR = "/opt/airflow/data/cleaned"
CLEAN_FILE = f"{CLEAN_DIR}/intel_cleaned.csv"


# -------------------------
# Preprocess (RAW → PREPROCESSED)
# -------------------------
def preprocess_data(ti):
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Intel data.txt not found")

    os.makedirs(PREPROCESS_DIR, exist_ok=True)

    df = pd.read_csv(
        RAW_FILE,
        sep=r"\s+",
        header=None,
        names=[
            "date", "time", "epoch",
            "nodeid", "temperature",
            "humidity", "light", "voltage"
        ]
    )

    # Basic preprocessing
    df["timestamp"] = pd.to_datetime(
        df["date"] + " " + df["time"],
        errors="coerce"
    )

    # Remove malformed rows
    df = df.dropna(subset=["timestamp"])

    # Save preprocessed output
    df.to_csv(PREPROCESS_FILE, index=False)

    ti.xcom_push(key="raw_rows", value=len(df))

    return "clean_data"


# -------------------------
# Clean Data + Branch
# -------------------------
def clean_data(ti):
    os.makedirs(CLEAN_DIR, exist_ok=True)

    df = pd.read_csv(PREPROCESS_FILE)

    # Cleaning rules (REALISTIC)
    df = df[df["temperature"].between(-10, 60)]
    df = df[df["humidity"].between(0, 100)]
    df = df[df["voltage"] > 2.0]
    df = df.dropna()

    cleaned_count = len(df)
    df.to_csv(CLEAN_FILE, index=False)

    ti.xcom_push(key="cleaned_rows", value=cleaned_count)

    if cleaned_count > 0:
        return "validate_data"
    else:
        return "no_data"


# -------------------------
# Validate Cleaned Data
# -------------------------
def validate_data(ti):
    cleaned_rows = ti.xcom_pull(
        task_ids="clean_data", key="cleaned_rows"
    )

    if cleaned_rows < 1000:
        raise ValueError("Validation failed: Not enough cleaned records")

    print(f"Validation successful with {cleaned_rows} rows")


# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="sprint2_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint2", "intel", "preprocess", "clean"],
) as dag:

    start = EmptyOperator(task_id="start")

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
    )

    clean = BranchPythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    no_data = EmptyOperator(task_id="no_data")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> preprocess >> clean
    clean >> validate >> end
    clean >> no_data >> end
