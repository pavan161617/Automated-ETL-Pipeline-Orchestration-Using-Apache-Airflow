from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime


def latest_successful_dag_run(execution_date, **context):
    dag_runs = DagRun.find(
        dag_id="sprint6_producer_dag",
        state=State.SUCCESS
    )
    if not dag_runs:
        return None

    return max(dr.execution_date for dr in dag_runs)


with DAG(
    dag_id="sprint6_consumer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint6", "consumer"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer_dag",
        external_dag_id="sprint6_producer_dag",
        external_task_id="end",
        execution_date_fn=latest_successful_dag_run,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    consume = EmptyOperator(task_id="consume_data")
    end = EmptyOperator(task_id="end")

    start >> wait_for_producer >> consume >> end
