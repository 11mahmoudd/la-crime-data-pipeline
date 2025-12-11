from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd


with DAG(
    dag_id='create_tables',
    tags=["crime"]


) as dag:

    create_crimes_table = SQLExecuteQueryOperator(
        task_id='create_crimes_table',
        conn_id='postgres_conn',
        sql='sql/create_crimes.sql'
    )
