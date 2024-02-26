# Автоматизированный Data Pipeline с использованием Apache Airflow и Spark

## Общее описание

Этот репозиторий содержит код DAG для Apache Airflow и соответствующий Scala-скрипт для Apache Spark. DAG автоматизирует процесс проверки наличия таблицы на кластере Hadoop и, в случае её существования, подсчёт количества строк в ней. Результаты работы DAG отправляются по электронной почте.

## Исходная задача

1. Написать даг, который сохранит агрегации подсчета user_id из папок regions_random в отдельные папки по регионам, партиционированные по дате.
2. Написать даг, который в формате mapped таски будет загружать аггрегированные данные по регионам в таблицу Postgre с помощью 
```
copy_expert:

cur = conn.cursor()
copy_sql = """
COPY table_name FROM stdin WITH CSV HEADER
DELIMITER as ','
"""
with open(path, 'r') as f:
cur.copy_expert(sql=copy_sql, file=f)
conn.commit()
cur.close()
```

## Инструкции по запуску

Для подключения использовать conn_id="postgres_data_{username}"
