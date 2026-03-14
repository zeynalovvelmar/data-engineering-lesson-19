from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


def failure_callback_message(context):
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    print(f"Xəta: {task_id} {execution_date}.")


def check_row_count_logic(**kwargs):
    target_date = kwargs["ds"]

    # KRİTİK DÜZƏLİŞ: Connection ID sensorla eyni olmalıdır
    hook = PostgresHook(postgres_conn_id="pg_conn_test")
    sql = f"SELECT COUNT(*) FROM public.transactions WHERE transaction_date = '{target_date}';"

    records = hook.get_first(sql)
    row_count = records[0] if records else 0

    print(f"Tarix: {target_date} üzrə sətir sayı: {row_count}")

    if row_count > 2:
        return "load_to_minio"
    else:
        return "fail_task"


def force_failure():
    raise AirflowFailException("Şərt ödənmədi: Sətir sayı 2-dən çox olmalıdır.")


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 10),
    "retries": 0,
}

with DAG(
    "transaction_pipeline_practice",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["practice", "postgres", "spark", "minio"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_yesterday_data = SqlSensor(
        task_id="check_yesterday_data",
        conn_id="pg_conn_test",
        sql="SELECT 1 FROM public.transactions WHERE transaction_date = '{{ ds }}'",
        mode="poke",
        poke_interval=60,
        timeout=600,
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=check_row_count_logic,
    )

    load_to_minio = BashOperator(
        task_id="load_to_minio",
        bash_command="spark-submit /opt/airflow/files/spark_job.py {{ ds }}",
    )

    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=force_failure,
        on_failure_callback=failure_callback_message,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> check_yesterday_data >> branch
    branch >> [load_to_minio, fail_task]
    [load_to_minio, fail_task] >> end
