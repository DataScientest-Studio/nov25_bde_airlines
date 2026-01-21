from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="airlines_collect",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["airlines", "collect"],
) as dag:

    collect = BashOperator(
        task_id="collect_airlabs_to_mongo",
        bash_command="python /opt/airflow/scripts/Collect_data.py",
    )
