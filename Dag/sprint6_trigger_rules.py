from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


def success_task():
    print("This task succeeds")


def fail_task():
    raise AirflowFailException("Intentional failure")


def cleanup_task():
    print("Cleanup task running regardless of upstream status")


def failure_handler():
    print("Failure handler triggered")


with DAG(
    dag_id="sprint6_trigger_rules",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sprint6", "trigger_rules"],
) as dag:

    start = EmptyOperator(task_id="start")

    success = PythonOperator(
        task_id="success_task",
        python_callable=success_task,
    )

    failure = PythonOperator(
        task_id="failure_task",
        python_callable=fail_task,
    )

    cleanup = PythonOperator(
        task_id="cleanup_task",
        python_callable=cleanup_task,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    on_failure = PythonOperator(
        task_id="on_failure_task",
        python_callable=failure_handler,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> [success, failure]
    [success, failure] >> cleanup
    [success, failure] >> on_failure
    cleanup >> end
