from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dbt_crime_models',
    tags=["crime"],
) as dag:

    dbt_run_stg = BashOperator(
        task_id='dbt_run_stg',
        bash_command="cd /usr/local/airflow/dbt && dbt clean && dbt run --select stg --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command="cd /usr/local/airflow/dbt && dbt clean && dbt run --select marts --profiles-dir .",
    )

    dbt_run_stg >> dbt_run_marts