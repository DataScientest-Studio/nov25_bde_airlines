from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airlines_dst",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airlines_collect_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",  # toutes les 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=["airlines", "etl"],
) as dag:

    collect = BashOperator(
        task_id="collect_airlabs_to_mongo",
        bash_command="python /opt/airflow/scripts/Collect_data.py",
    )

    etl = BashOperator(
        task_id="mongo_to_postgres",
        bash_command="python /opt/airflow/scripts/mongo_to_sql.py",
    )

    collect >> etl
