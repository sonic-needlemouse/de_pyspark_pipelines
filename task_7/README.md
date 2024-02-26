# Автоматизированный Data Pipeline с использованием Apache Airflow и Spark

## Общее описание

Этот репозиторий содержит код DAG для Apache Airflow и соответствующий Scala-скрипт для Apache Spark. DAG автоматизирует процесс проверки наличия таблицы на кластере Hadoop и, в случае её существования, подсчёт количества строк в ней. Результаты работы DAG отправляются по электронной почте.

## Исходная задача

1. Написать даг, который будет считать с помощью spark считать агрегаты по регионам
и загружать в postgre через jdbc. Пример загрузки данных в postgre:
```
.format("jdbc")
.option("driver","org.postgresql.Driver")
.option("url", f"jdbc:postgresql://postgres-data:5432/{database}")
.option("dbtable", target_table)
.option("user", username)
.option("password", password)
.mode('append')
.save()
```
Даг должен быть идемпотентным, то есть перезапуск определенной даты должен полностью
обновлять данные за соответствующи интервал.
