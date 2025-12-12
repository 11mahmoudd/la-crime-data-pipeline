from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def validate_stg_counts():
    """Verify staging table has data"""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    stg_count = hook.get_first("SELECT COUNT(*) FROM stg.stg_crime")[0]
    
    if stg_count == 0:
        raise ValueError("No data in stg.stg_crime table")
    
    print(f"✓ Staging validation passed: {stg_count:,} records")



def validate_fact_counts():
    """Verify fact table has data and matches staging"""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    fact_count = hook.get_first("SELECT COUNT(*) FROM dwh.fact_crime")[0]
    stg_count = hook.get_first("SELECT COUNT(*) FROM stg.stg_crime")[0]
    
    if fact_count == 0:
        raise ValueError("No data in fact_crime table")
    
    # Allow some variance due to incremental loads
    variance = abs(fact_count - stg_count)
    if variance > stg_count * 0.1:  # More than 10% difference
        raise ValueError(f"Large discrepancy: fact={fact_count:,}, stg={stg_count:,}")
    
    print(f"✓ Fact table validation passed: {fact_count:,} records")


def truncate_staging():
    """Truncate staging table to free up space"""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    hook.run("TRUNCATE TABLE stg.stg_crime")
    print("✓ Staging table truncated")


with DAG(
    dag_id='dbt_crime_models',
    tags=["crime"],
) as dag:

    # dbt_deps = BashOperator(
    #     task_id='install_dependencies',
    #     bash_command="cd /usr/local/airflow/dbt && dbt deps --profiles-dir .",
    # )

    # dbt_run_stg = BashOperator(
    #     task_id='run_stg',
    #     bash_command="cd /usr/local/airflow/dbt && dbt run --select stg --profiles-dir .",
    # )

    # validate_stg = PythonOperator(
    # task_id='validate_staging',
    # python_callable=validate_stg_counts
    # )

    # dbt_test_stg = BashOperator(
    #     task_id='test_stg',
    #     bash_command="cd /usr/local/airflow/dbt && dbt test --select stg --profiles-dir .",
    # )

    dbt_run_marts = BashOperator(
        task_id='run_marts',
        bash_command="cd /usr/local/airflow/dbt && dbt run --select marts --profiles-dir .",
    )

    dbt_test_marts = BashOperator(
        task_id='test_marts',
        bash_command="cd /usr/local/airflow/dbt && dbt test --select marts --profiles-dir .",
    )

    validate_fact = PythonOperator(
        task_id='validate_fact',
        python_callable=validate_fact_counts
    )

    truncate_staging = PythonOperator(
        task_id='truncate_staging',
        python_callable=truncate_staging
    )

    # dbt_deps >> dbt_run_stg >> validate_stg >> dbt_test_stg >>
    dbt_run_marts >> validate_fact >> dbt_test_marts >> truncate_staging