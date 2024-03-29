# [Apache PySpark+Airflow](https://t1.ru/internship/item/otkrytaya-shkola-dlya-inzhenerov-dannykh-data-engineer/). Репозиторий проектов.

![logo-wide](spark_airflow.jpeg)

## Описание

Открытая школа – это практический курс, рассчитанный на 16 часов вебинаров и 32 часа самостоятельной практики, включая финальный проект.

Проект включает в себя разработку ETL-пайплайнов для обработки и анализа больших данных. Используя инструменты Apache PySpark, реализованы решения для обработки данных в рамках Hadoop экосистемы. AirFlow используется для организации и автоматизации рабочих процессов.

## Технологии
- Apache Spark (Python API)
- Hadoop
- Apache AirFlow
- Python
- SQL

## Структура

| Номер проекта                | Название проекта                          | Краткое описание                                                                                                     |
|------------------------------|-------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| [Задача_0](task_0)       | Задание на проверку соединения и доступа  | Вводное задание (проверка соединения и доступа) на проверку доступа к Spark кластеру.                                |
| [Задача_1](task_1)       | Задание на аггрегирующие функции (Pyspark)  | Подсчет посуточных аггрегатов для данных по различным категориям.                                            |
| [Задача_2](task_2)       | Задание на аггрегирующие функции Pyspark и дальнейший графический анализ (Pyspark)  | Подсчет аггрегирующих функция для различных фичей и построение графиков с помощью функции z.show()               |
| [Задача_3](task_3)       | Задания на оптимизацию запросов & catalyst optimizator & salting (Pyspark)        | Получение плана запросов на различных (по размеру) датафреймах и дальнейшая оптимизация запросов используя catalyst |
| [Задача_4](task_4)       | RDD & Анализ данных популярных песен и артистов (Pyspark) | Анализ данных о популярных песнях и артистах с использованием Spark, включая оптимизацию запросов.                   |
| [Задача_5](task_5)       | Автоматизированный Data Pipeline (Airflow)         | Создание DAG в Airflow для автоматизации процесса создания папки на дневной основе и дальнейшее сохранение отфильтрованных данных в эти папки на          |
| [Задача_6](task_6)       | Автоматизированный Data Pipeline (Airflow)         | Создание DAG в Airflow для автоматизации процесса подсчета аггрегатов и сохранения результатов в БД postgres |
| [Задача_7](task_7)       | Автоматизированный Data Pipeline (Airflow + Spark)         | Создание DAG в Airflow, который будет считать с помощью spark считать агрегаты по регионам и загружать в postgre через jdbc. Даг должен быть идемпотентным |