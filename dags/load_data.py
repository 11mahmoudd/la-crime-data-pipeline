from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


def process_data():
    selected_cols = [
    "DATE OCC",
    "TIME OCC",
    "AREA",
    "AREA NAME",
    "Crm Cd",
    "Crm Cd Desc",
    "Vict Age",
    "Vict Sex",
    "Status",
    "Status Desc"
    ]
    df = pd.read_csv("/usr/local/airflow/data/Crime_Data_from_2020_to_Present.csv",usecols=selected_cols)


    df.to_csv("/usr/local/airflow/data/Processed_Crime_Data.csv", index=False)

def clean_data():
    df_selected = pd.read_csv("/usr/local/airflow/data/Processed_Crime_Data.csv")
    
    df_selected["Vict Sex"].fillna("Unknown", inplace=True)
    
    # Replace H and - with Unknown, keep only M, F, X, Unknown
    df_selected["Vict Sex"] = df_selected["Vict Sex"].replace(["H", "-"], "Unknown")
    
    # Convert TIME OCC from integer (845, 1530) to time format (08:45:00, 15:30:00)
    df_selected['TIME OCC'] = pd.to_datetime(df_selected['TIME OCC'].astype(str).str.zfill(4), format='%H%M', errors='coerce').dt.time
    
    df_selected.dropna(inplace=True)
    df_selected.to_csv("/usr/local/airflow/data/Processed_Crime_Data.csv", index=False)


def load_data():
    from sqlalchemy.types import Date, Integer, Text, Time
    
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()

    # Define explicit data types for columns
    dtype_mapping = {
        'date_occ': Date,
        'time_occ': Time,
        'area': Integer,
        'area_name': Text,
        'crm_cd': Integer,
        'crm_cd_desc': Text,
        'vict_age': Integer,
        'vict_sex': Text,
        'status': Text,
        'status_desc': Text
    }

    # Read and insert in small chunks – never loads the whole 1M rows at once
    for chunk in pd.read_csv(
        "/usr/local/airflow/data/Processed_Crime_Data.csv",
        chunksize=50_000,          
        low_memory=False
    ):
        chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')
        
        # Convert time_occ from integer (845, 1530) to time format (08:45:00, 15:30:00)
        
        
        chunk.to_sql(
            name="raw_crimes",
            con=engine,
            schema="raw",
            if_exists="append",     
            index=False,
            method="multi",     
            chunksize=10_000,
            dtype=dtype_mapping
        )
        print(f"Inserted {len(chunk):,} rows – total so far ≈ {chunk.index[-1]+1:,}")



with DAG(
    dag_id='load_to_postgres',
    tags=["crime"]


) as dag:

    process_data = PythonOperator(
            task_id='process_data_task',
            python_callable=process_data
        )

    clean_data = PythonOperator(
        task_id='clean_data_task',
        python_callable=clean_data
    )

    load_data = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data
    )

    process_data >> clean_data >> load_data


