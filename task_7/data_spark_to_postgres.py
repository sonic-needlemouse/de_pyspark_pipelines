from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
import csv
from pathlib import Path
from airflow.models import Variable
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CONN_ID = 'private_info'
DATA_DIR = '/opt/airflow/data/regions_random/'
jars_path = 'jars/postgresql-42.3.1.jar'
connection = BaseHook.get_connection(CONN_ID)

DEFAULT_ARGS = {
    "owner": "private_info",
    "email": "private_info@mail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    'start_date': datetime(2024, 2, 1),
#    'end_date': datetime(2022, 3, 14),
}

with DAG(dag_id="data_spark_to_postgres",
         description="Write data to postgres",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         default_args=DEFAULT_ARGS,
         tags=['lesson_3', 'private_info']
         ) as dag:
    
    
    jars_path = 'jars/postgresql-42.3.1.jar'
    url = f"postgresql://postgres:5432/postgres"
    
    @task
    def get_regions_to_aggregate(**kwargs):
        regions_to_aggretate = []
        p = Path(DATA_DIR)

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path("/opt/airflow/").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(f"filepath={filepath}")
            print(f"is_file={filepath.is_file()}")
            if filepath.is_file():
                regions_to_aggretate.append(r)
                print("Appended")

        regions_list = [str(p).split("/")[-1] for p in regions_to_aggretate]
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
           CREATE TABLE IF NOT EXISTS users_agg_spark (
           user_id INT,
           count INT,
           date DATE,
           region VARCHAR
           );
           """
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        print("Table created")
      
    
    @task
    def aggregate_daily_data(region, **kwargs):
        print(f"region = {region}")
        filename = f"{DATA_DIR}/{region}/{kwargs['ds']}.csv" 
        print(f"filename = {filename}")
        

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM users_agg_spark WHERE date='{kwargs['ds']}' AND region='{region}'")
        conn.commit()
        cur.close()
        print(f"Data deleted for {region} {kwargs['ds']}")
        
        spark = SparkSession.builder.appName("Data Processing").config('spark.jars', jars_path).getOrCreate()

        df = spark.read.option("inferSchema", 'True')\
                  .csv(filename, header=True)\
                  .groupBy("user_id")\
                  .agg(F.count("*").alias("count"))\
                  .withColumn('date', F.to_date(F.lit(str(kwargs['ds'])))) \
                  .withColumn('region', F.lit(region))
            
        df.write \
            .format("jdbc") \
            .option("driver","org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://postgres-data:5432/{connection.schema}") \
            .option("dbtable", "users_agg_spark") \
            .option("user", connection.login) \
            .option("password", connection.password) \
            .mode('append') \
            .save()

        spark.stop()
      
        
    regions_list = get_regions_to_aggregate()
    create_table()
    aggregate_daily_data.expand(region=regions_list)
