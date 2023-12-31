"""Apache Airflow DAG to setup the database schema in Postgres database and import the static staging data"""

import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageCsvToPostgresOperator
from airflow.operators import StageParquetToPostgresOperator
from helpers import PostgresSqlQueries

DATA_LAKE_PATH = "/data/datalake"
DATABASE_CONNECTION_ID = "udacity-capstone-postgres"

default_args = {"owner": "Udacity DE ND"}

dag = DAG('setup_postgres',
          default_args=default_args,
          description='Creating table schema in database and load static staging data',
          schedule_interval=None,
          start_date=datetime.datetime(2022, 7, 12),
          concurrency=2
          )

create_staging_immigration_table = PostgresOperator(
    task_id="create_staging_immigration_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_staging_immigration
)

create_staging_countries_table = PostgresOperator(
    task_id="create_staging_countries_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_staging_countries
)

create_staging_ports_table = PostgresOperator(
    task_id="create_staging_ports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_staging_ports_table
)

create_staging_airports_table = PostgresOperator(
    task_id="create_staging_airports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_staging_airports_table
)

create_staging_demographics_table = PostgresOperator(
    task_id="create_staging_demographics_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_staging_demographics_table
)

create_countries_table = PostgresOperator(
    task_id="create_countries_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_countries_table
)

create_ports_table = PostgresOperator(
    task_id="create_ports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_ports_table
)

create_airports_table = PostgresOperator(
    task_id="create_airports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_airports_table
)

create_demographics_table = PostgresOperator(
    task_id="create_demographics_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_demographics_table
)

create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_time_table
)

create_fact_immigration_table = PostgresOperator(
    task_id="create_fact_immigration_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=PostgresSqlQueries.create_fact_immigration_table
)

stage_port_codes_task = StageCsvToPostgresOperator(
    task_id="stage_port_codes",
    postgres_conn_id=DATABASE_CONNECTION_ID,
    dag=dag,
    table_name="public.staging_ports",
    csv_path=f"{DATA_LAKE_PATH}/i94_ports.csv",
    truncate_table=True
)

stage_country_codes_task = StageCsvToPostgresOperator(
    task_id="stage_country_codes",
    postgres_conn_id=DATABASE_CONNECTION_ID,
    dag=dag,
    table_name="public.staging_countries",
    csv_path=f"{DATA_LAKE_PATH}/i94_countries.csv",
    truncate_table=True
)

stage_airports_codes_task = StageCsvToPostgresOperator(
    task_id="stage_airports_codes",
    postgres_conn_id=DATABASE_CONNECTION_ID,
    dag=dag,
    table_name="public.staging_airports",
    csv_path=f"{DATA_LAKE_PATH}/airport-codes_csv.csv",
    truncate_table=True
)

stage_demographics_task = StageCsvToPostgresOperator(
    task_id="stage_demographics",
    postgres_conn_id=DATABASE_CONNECTION_ID,
    dag=dag,
    table_name="public.staging_demographics",
    csv_path=f"{DATA_LAKE_PATH}/us-cities-demographics.csv",
    truncate_table=True,
    delimiter=";"
)

stage_city_temperatures_task = StageParquetToPostgresOperator(
    task_id="stage_city_temperatures",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    parquet_path=f"{DATA_LAKE_PATH}/us_city_temperature_data",
    table_name="staging_city_temperatures",
    truncate_table=True
)

stage_country_temperatures_task = StageParquetToPostgresOperator(
    task_id="stage_country_temperatures",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    parquet_path=f"{DATA_LAKE_PATH}/country_temperature_data",
    table_name="staging_country_temperatures",
    truncate_table=True
)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_staging_immigration_table
start_operator >> create_staging_countries_table
start_operator >> create_staging_ports_table
start_operator >> create_staging_airports_table
start_operator >> create_staging_demographics_table
start_operator >> stage_country_temperatures_task
start_operator >> stage_city_temperatures_task
start_operator >> create_time_table
start_operator >> create_ports_table
start_operator >> create_countries_table

create_ports_table >> create_airports_table
create_ports_table >> create_demographics_table

create_staging_countries_table >> stage_country_codes_task
create_staging_ports_table >> stage_port_codes_task
create_staging_airports_table >> stage_airports_codes_task
create_staging_demographics_table >> stage_demographics_task

create_time_table >> create_fact_immigration_table
create_ports_table >> create_fact_immigration_table
create_countries_table >> create_fact_immigration_table

create_staging_immigration_table >> end_operator
stage_country_codes_task >> end_operator
stage_port_codes_task >> end_operator
stage_airports_codes_task >> end_operator
stage_demographics_task >> end_operator
stage_city_temperatures_task >> end_operator
stage_country_temperatures_task >> end_operator
