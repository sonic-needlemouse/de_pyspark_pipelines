from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


ARGS = {
    "owner": "private_info",
    "start_date": datetime(2024, 2, 17),
}


with DAG(
    dag_id="create_folder",
    default_args=ARGS,
    description="DAG for directory creation",
    schedule_interval="@once",
    tags=["lesson_1", "private_info"],
) as dag:
    create_directory_task = BashOperator(
        task_id="create_directory_task",
        bash_command="mkdir -p /opt/airflow/data/private_info",
        dag=dag,
    )
