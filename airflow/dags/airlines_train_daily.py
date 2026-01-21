from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airlines_dst",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="airlines_train_daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="15 18 * * *",  # 18:15 tous les jours
    catchup=False,
    max_active_runs=1,
    tags=["airlines", "ml"],
) as dag:

    train = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/scripts/train_ml.py",
    )
