from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from pathlib import Path
import csv
from pathlib import Path
import logging


CONN_ID = 'private_info'
DATA_DIR = '/opt/airflow/data/regions_random/'
ENDPOINT_DIR = '/opt/airflow/data/private_info/regions_random/'


DEFAULT_ARGS = {
    "owner": "private_info",
    "email": "private_info@mail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    'start_date': datetime(2024, 1, 1),
#    'end_date': datetime(2022, 3, 14),
}

with DAG(dag_id="aggregate_daily_data",
         description="Aggregating user_id per date",
         start_date=datetime(2024, 1, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         default_args=DEFAULT_ARGS,
         tags=['lesson_2', 'private_info']
         ) as dag:
    
    @task
    def get_regions_to_aggregate(**kwargs):
        regions_to_aggretate = []
        p = Path(DATA_DIR)

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path(f"{DATA_DIR}").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            #filepath = Path("/opt/airflow/").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(f"filepath={filepath}")
            print(f"is_file={filepath.is_file()}")
            if filepath.is_file():
                regions_to_aggretate.append(r)
                print("Appended")
        
        regions_list = [str(p).split("/")[-1] for p in regions_to_aggretate]
        print(f'regions_list: {regions_list}')
        #logging.info(regions_list)
        
        return regions_list



    @task
    def aggregate_daily_data(region, **kwargs):
        print(f"region = {region}")
        filename = f"{DATA_DIR}/{region}/{kwargs['ds']}.csv" 
        print(f"filename = {filename}")
        user_count = {}
        with open(filename, newline='\n') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                if row[0] == 'user_id':
                    continue
                user_id = row[0]
                if user_id in user_count:
                    user_count[user_id] = user_count[user_id] + 1
                else:
                    user_count[user_id] = 1

        filename_agg = f"{ENDPOINT_DIR}/{region}/{kwargs['ds']}.csv"
        Path(filename_agg).parent.mkdir(parents=True, exist_ok=True)
        with open(filename_agg, "w") as file:
            file.write("user_id, count\n")
            for user_id, val_count in user_count.items():
                file.write(f"{user_id}, {val_count}\n")
        
        print(f"Write file {filename_agg}")
        return filename_agg


    regions_list = get_regions_to_aggregate()

    write_values = aggregate_daily_data.expand(region=regions_list)
