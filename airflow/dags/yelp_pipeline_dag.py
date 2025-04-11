# airflow/dags/yelp_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tasks.data_fetch import fetch_yelp_data
from tasks.data_process import run_data_processing
from tasks.sentiment_analysis import run_sentiment_analysis
from tasks.trend_analysis import run_topic_modeling
from tasks.snowflake_sync import run_snowflake_sync

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'yelp_sentiment_pipeline',
    default_args=default_args,
    description='Yelp sentiment analysis pipeline',
    schedule_interval=timedelta(days=7),
    catchup=False
) as dag:
    
    # Task 1: Fetch data from Yelp API
    fetch_data_task = PythonOperator(
        task_id='fetch_yelp_data',
        python_callable=fetch_yelp_data,
        op_kwargs={
            'api_key': '{{ var.value.yelp_api_key }}',
            'location': 'Boston, MA',
            'categories': 'restaurants',
            'limit': 50,
            'output_dir': '/opt/airflow/data/raw'
        }
    )
    
    # Task 2: Process data
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=run_data_processing,
        op_kwargs={
            'business_path': '/opt/airflow/data/raw/businesses.json',
            'review_path': '/opt/airflow/data/raw/reviews.json',
            'output_dir': '/opt/airflow/data/processed'
        }
    )
    
    # Task 3: Run sentiment analysis
    sentiment_analysis_task = PythonOperator(
        task_id='sentiment_analysis',
        python_callable=run_sentiment_analysis,
        op_kwargs={
            'review_paths': "{{ ti.xcom_pull(task_ids='process_data')['review_paths'] }}",
            'output_dir': '/opt/airflow/data/sentiment'
        }
    )
    
    # Task 4: Run trend analysis
    trend_analysis_task = PythonOperator(
        task_id='trend_analysis',
        python_callable=run_topic_modeling,
        op_kwargs={
            'review_paths': "{{ ti.xcom_pull(task_ids='process_data')['review_paths'] }}",
            'output_dir': '/opt/airflow/data/trends'
        }
    )
    
    # Task 5: Sync data to Snowflake
    snowflake_sync_task = PythonOperator(
        task_id='snowflake_sync',
        python_callable=run_snowflake_sync,
        op_kwargs={
            'config_path': '/opt/airflow/config/snowflake.json',
            'business_path': "{{ ti.xcom_pull(task_ids='process_data')['selected_business_path'] }}",
            'review_paths': "{{ ti.xcom_pull(task_ids='process_data')['review_paths'] }}",
            'sentiment_path': "{{ ti.xcom_pull(task_ids='sentiment_analysis')['sentiment_path'] }}",
            'trend_summary_path': "{{ ti.xcom_pull(task_ids='trend_analysis')['trend_summary_path'] }}"
        }
    )
    
    # Define task dependencies
    fetch_data_task >> process_data_task >> sentiment_analysis_task >> snowflake_sync_task
    process_data_task >> trend_analysis_task >> snowflake_sync_task