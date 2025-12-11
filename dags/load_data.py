from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd




def validate_csv_structure():
    """Validate CSV has required columns before processing"""
    import pandas as pd
    
    required_columns = [
        "DATE OCC", "TIME OCC", "AREA", "AREA NAME",
        "Crm Cd", "Crm Cd Desc", "Vict Age", "Vict Sex",
        "Status", "Status Desc"
    ]
    
    df = pd.read_csv("/usr/local/airflow/data/Crime_Data_from_2020_to_Present.csv", nrows=1)
    missing_cols = set(required_columns) - set(df.columns)
    
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    print("✓ CSV structure validation passed")
    df = pd.read_csv("/usr/local/airflow/data/Crime_Data_from_2020_to_Present.csv",usecols=required_columns)


def validate_data_quality():
    """Check data quality metrics"""
    import pandas as pd
    
    df = pd.read_csv("/usr/local/airflow/data/Processed_Crime_Data.csv")
    # Fill missing Vict Sex with 'Unknown'
    df["Vict Sex"].fillna("Unknown", inplace=True)
    
    # Replace H and - with Unknown, keep only M, F, X, Unknown
    df["Vict Sex"] = df["Vict Sex"].replace(["H", "-"], "Unknown")
    
    # Convert TIME OCC from integer (845, 1530) to time format (08:45:00, 15:30:00)
    df['TIME OCC'] = pd.to_datetime(df['TIME OCC'].astype(str).str.zfill(4), format='%H%M', errors='coerce').dt.time
    

    # Check for nulls in critical columns
    null_checks = {
        'DATE OCC': df['DATE OCC'].isnull().sum(),
        'AREA': df['AREA'].isnull().sum(),
        'Crm Cd': df['Crm Cd'].isnull().sum()
    }
    
    for col, null_count in null_checks.items():
        if null_count > 0:
            raise ValueError(f"Column {col} has {null_count} null values")
        
    # Drop rows with any remaining nulls
    df.dropna(inplace=True)


    # Check date range
    df['DATE OCC'] = pd.to_datetime(df['DATE OCC'])
    if df['DATE OCC'].min() < pd.Timestamp('2020-01-01'):
        raise ValueError(f"Data contains dates before 2020: {df['DATE OCC'].min()}")
    
    if df['DATE OCC'].max() > pd.Timestamp.now():
        raise ValueError(f"Data contains future dates: {df['DATE OCC'].max()}")
    
    print(f"✓ Data quality validation passed")
    print(f"  - Records: {len(df):,}")
    print(f"  - Date range: {df['DATE OCC'].min()} to {df['DATE OCC'].max()}")

    df.to_csv("/usr/local/airflow/data/Processed_Crime_Data.csv", index=False)


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

def validate_load_counts():
    """Verify data was loaded to raw table"""
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    # Check raw table count
    raw_count = hook.get_first("SELECT COUNT(*) FROM raw.raw_crime")[0]
    
    if raw_count == 0:
        raise ValueError("No data loaded to raw.raw_crime table")
    
    print(f"✓ Load validation passed: {raw_count:,} records in raw table")



with DAG(
    dag_id='load_to_postgres',
    tags=["crime"]


) as dag:

    validate_csv = PythonOperator(
            task_id='validate_csv_task',
            python_callable=validate_csv_structure
        )

    validate_data = PythonOperator(
        task_id='validate_data_task',
        python_callable=validate_data_quality
    )


    load_to_postgres = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data
    )

    validate_load = PythonOperator(
        task_id='validate_load_counts_task',
        python_callable=validate_load_counts
    )

    validate_csv >> validate_data >> load_to_postgres >> validate_load


