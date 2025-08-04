from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'ryan',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='emissions_pipeline',
    default_args=default_args,
    description='End-to-end emissions pipeline using PySpark + dbt',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Step 1: Ingest raw data from CSV in MinIO
    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command='spark-submit /opt/airflow/pyspark_jobs/ingest_raw_data.py',
    )

    # Step 2: Clean data (nulls, date formats, bad values)
    clean_data = BashOperator(
        task_id='clean_data',
        bash_command='spark-submit /opt/airflow/pyspark_jobs/clean_data.py',
    )

    # Step 3: Apply business logic and output Parquet
    process_logic = BashOperator(
        task_id='process_business_logic',
        bash_command='spark-submit /opt/airflow/pyspark_jobs/process_business_logic.py',
    )

    # Step 4: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt && dbt run',
    )

    # DAG order
    ingest_data >> clean_data >> process_logic >> dbt_run
    print("DAG created successfully with tasks: ingest_data, clean_data, process_logic, dbt_run")