from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="logsearch_bronze_silver_gold",
    start_date=datetime(2026, 7, 1),
    schedule=None,  # chạy tay (manual trigger) trước, chưa đặt lịch tự động
    catchup=False,
    tags=["logsearch", "lakehouse"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="cd /opt/airflow && python scripts/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="cd /opt/airflow && python scripts/silver_to_gold.py",
    )

    bronze_to_silver >> silver_to_gold