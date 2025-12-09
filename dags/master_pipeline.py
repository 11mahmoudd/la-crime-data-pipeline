from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='master_crime_pipeline',
    tags=["crime", "master"],
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False
) as dag:

    trigger_create_tables = TriggerDagRunOperator(
        task_id='trigger_create_tables',
        trigger_dag_id='create_tables',
        wait_for_completion=True,  # Wait for DAG to complete
        poke_interval=30,  # Check status every 30 seconds
        allowed_states=['success'],  # Only proceed if successful
        failed_states=['failed']
    )

    trigger_load_data = TriggerDagRunOperator(
        task_id='trigger_load_data',
        trigger_dag_id='load_to_postgres',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt_crime_models',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # Define execution order
    trigger_create_tables >> trigger_load_data >> trigger_dbt
