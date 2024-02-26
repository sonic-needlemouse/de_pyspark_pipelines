from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
from pathlib import Path
import logging

CONN_ID = 'private_info'
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

with DAG(dag_id="data_to_postgres",
         description="Write data to postgres",
         start_date=datetime(2024, 1, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         default_args=DEFAULT_ARGS,
         tags=['lesson_2', 'private_info']
         ) as dag:
    
    
    @task
    def get_regions_for_write(**kwargs):
        regions_to_write = []
        p = Path(ENDPOINT_DIR)

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path(f"{ENDPOINT_DIR}").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(f"filepath={filepath}")
            print(f"is_file={filepath.is_file()}")
            if filepath.is_file():
                regions_to_write.append(r)
                print("Appended")
        
        regions_list = [str(p).split("/")[-1] for p in regions_to_write]
        print(f'regions_list: {regions_list}')
        
        return regions_list
    
    
    @task
    def create_table(**kwargs):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        try:
            conn = hook.get_conn()
            cur = conn.cursor()
        except Exception as e:
            print("Connection failed")
            print(e.message, e.args)
        
        create_table_sql = """
           CREATE TABLE IF NOT EXISTS users_aggregations (
           date DATE,
           region VARCHAR,
           user_id INT,
           count INT
           );
           """
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        print("Table created")
    
    
    @task
    def write_data_to_postgres(region, **kwargs):
        print(f"region = {region}")
        filename = f"{ENDPOINT_DIR}/{region}/{kwargs['ds']}.csv" 
        print(f"filename = {filename}")
        
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        try:
            conn = hook.get_conn()
            cur = conn.cursor()
        except Exception as e:
            print("Connection failed")
            print(e.message, e.args)
        
        cur.execute(f"DELETE FROM users_aggregations WHERE date='{kwargs['ds']}' AND region='{region}'")
        
        copy_sql = f"""
            CREATE TEMPORARY TABLE buffer (
                user_id INT,
                count INT
            );
            
            COPY buffer FROM STDIN WITH CSV HEADER DELIMITER as ',';
            
            INSERT INTO users_aggregations (date, region, user_id, count)
            SELECT '{kwargs['ds']}', '{region}', user_id, count FROM buffer;
            
            DROP TABLE buffer;
            """
        
        with open(filename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            cur.close()
            print("Data writed")
            


    regions_list = get_regions_for_write()
    create_table()
    write_data_to_postgres.expand(region=regions_list)
