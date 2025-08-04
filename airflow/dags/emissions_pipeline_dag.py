from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# DAG Defaults
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['nguyentranhoan.mr@gmail.com'],  
}


dag = DAG(
    dag_id='ryan_emissions_pipeline',
    default_args=default_args,
    description='Full emissions pipeline using PySpark and MinIO',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Helper to build spark-submit commands
def spark_submit_command(script_path: str) -> str:
    return (
        'set -euo pipefail; '
        'docker exec spark-master spark-submit '
        '--master spark://spark-master:7077 '
        '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
        '--conf spark.hadoop.fs.s3a.access.key=minioadmin '
        '--conf spark.hadoop.fs.s3a.secret.key=minioadmin '
        '--conf spark.hadoop.fs.s3a.path.style.access=true '
        '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
        '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
        f'{script_path}'
    )

# DAG Tasks
pre_step = BashOperator(
    task_id='pre_step',
    bash_command='set -euo pipefail; echo "[PRE-STEP] Starting ..."',
    dag=dag,
)

ingest_raw_data = BashOperator(
    task_id='ingest_raw_data',
    bash_command=spark_submit_command('/opt/bitnami/spark/jobs/ingest_raw_data.py'),
    dag=dag,
)

ingest_stage_data = BashOperator(
    task_id='ingest_stage_data',
    bash_command=spark_submit_command('/opt/bitnami/spark/jobs/ingest_stage_data.py'),
    dag=dag,
)

transform_logic = BashOperator(
    task_id='transform_logic',
    bash_command=spark_submit_command('/opt/bitnami/spark/jobs/transform_logic.py'),
    dag=dag,
)

quality_check = BashOperator(
    task_id='run_tests',
    bash_command=spark_submit_command('/opt/bitnami/spark/jobs/check_emissions_quality.py'),
    dag=dag,
)

write_outputs = BashOperator(
    task_id='write_outputs',
    bash_command=spark_submit_command('/opt/bitnami/spark/jobs/write_outputs.py'),
    dag=dag,
)

post_step = BashOperator(
    task_id='post_step',
    bash_command='set -euo pipefail; echo "[POST-STEP] completed!"',
    dag=dag,
)

# DAG Flow
pre_step >> ingest_raw_data >> ingest_stage_data >> transform_logic >> write_outputs >> post_step

# pre_step >> ingest_raw_data >> ingest_stage_data >> transform_logic >> quality_check >> write_outputs >> post_step
