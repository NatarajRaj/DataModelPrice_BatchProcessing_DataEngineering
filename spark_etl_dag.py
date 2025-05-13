from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Add the path to your custom modules
sys.path.append(r'C:\Users\Edify\PycharmProjects\DataPriceModelProject\DataIngestion1')

# Import your process functions
from Bronze import process_bronze_data
from Silver import process_silver_data
from Gold import process_gold_data
from GoldDataVisualization import visualize_gold_data

def run_spark_job():
    process_bronze_data()
    process_silver_data()
    process_gold_data()
    visualize_gold_data()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 13),
    'retries': 1,
}

dag = DAG(
    'spark_etl_dag',
    default_args=default_args,
    description='An ETL DAG that runs Spark jobs',
    schedule_interval=None,
)

run_etl_task = PythonOperator(
    task_id='run_spark_etl',
    python_callable=run_spark_job,
    dag=dag,
)

