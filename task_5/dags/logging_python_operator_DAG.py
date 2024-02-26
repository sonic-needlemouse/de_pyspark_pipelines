from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

ARGS = {
    "owner": "private_info",
    "start_date": datetime(2024, 2, 17),
}


def print_kwargs(**kwargs):
    print("kwargs: ")
    for k, v in kwargs.items():
        print(f"key: {k}, value: {v}")


with DAG(
    dag_id="logging_python_operator_DAG",
    description="DAG for logging context from PythonOperator",
    schedule="@once",
    default_args=ARGS,
    tags=["lesson_1", "private_info"],
) as dag:

    task = PythonOperator(
        task_id="print_kwargs", python_callable=print_kwargs, provide_context=True
    )
