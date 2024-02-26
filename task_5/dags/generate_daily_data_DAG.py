from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import logging
import random
import time


ARGS = {
    "owner": "private_info",
    "start_date": datetime(2024, 2, 17),
}


def generate_daily_data(**kwargs):
    time.sleep(3)
    filename = f"/opt/airflow/data/private_info/calls/{kwargs['ds']}.csv"
    rows_count = 100000
    regions = ["MSK", "SPB", "KHB", "POV", "SIB", "SOU"]
    regions_weighs = [50, 30, 10, 4, 3, 3]

    with open(filename, "w") as file:
        header = "user_id,call_region,call_time\n"
        file.write(header)

        for i in range(rows_count):
            file.write(
                f"{random.randint(1, 100)},{random.choices(regions, weights=regions_weighs)[0]},{random.randrange(1000)}\n"
            )

    kwargs["ti"].xcom_push(key="key", value="value")
    return filename


with DAG(
    dag_id="daily_data_generation",
    schedule="@daily",
    max_active_runs=2,
    concurrency=2,
    default_args=ARGS,
    tags=["lesson_1", "private_info"],
) as dag:
    mkdir = BashOperator(
        task_id="mkdir", bash_command="mkdir -p /opt/airflow/data/private_info/calls"
    )

    generate_daily_data = PythonOperator(
        task_id="daily_data_generation",
        python_callable=generate_daily_data,
        provide_context=True,
        do_xcom_push=True,
    )

    get_daily_data_size = BashOperator(
        task_id="file_size_information",
        bash_command="wc -c /opt/airflow/data/private_info/calls/{{ ti.xcom_pull(task_ids='generate_daily_data', key='return_value') }}  | awk '{print $1}'",
        do_xcom_push=True,
    )

    mkdir >> generate_daily_data >> get_daily_data_size
