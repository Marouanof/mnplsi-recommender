from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='spark_training_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'training'],
) as dag:
    train_als_model = BashOperator(
        task_id='train_als_model',
        bash_command='python3 /opt/spark/work-dir/spark_als_training.py',
    )