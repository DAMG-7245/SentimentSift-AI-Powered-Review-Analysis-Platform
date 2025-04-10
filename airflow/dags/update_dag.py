# airflow/dags/update_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tasks.data_fetch import fetch_yelp_data_updates
from tasks.sentiment_analysis import run_sentiment_analysis
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
    'yelp_data_update',
    default_args=default_args,
    description='Update Yelp data and sentiment scores',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    # Task 1: Fetch updates from Yelp API
    fetch_updates_task = PythonOperator(
        task_id='fetch_yelp_updates',
        python_callable=fetch_yelp_data_updates,
        op_kwargs={
            'api_key': '{{ var.value.yelp_api_key }}',
            'business_ids': "{{ var.value.selected_business_ids }}",
            'output_dir': '/opt/airflow/data/updates'
        }
    )
    
    # Task 2: Run sentiment analysis on new reviews
    update_sentiment_task = PythonOperator(
        task_id='update_sentiment',
        python_callable=run_sentiment_analysis,
        op_kwargs={
            'review_paths': "{{ ti.xcom_pull(task_ids='fetch_yelp_updates')['review_paths'] }}",
            'output_dir': '/opt/airflow/data/sentiment_updates'
        }
    )
    
    # Task 3: Sync updated data to Snowflake
    update_snowflake_task = PythonOperator(
        task_id='update_snowflake',
        python_callable=run_snowflake_sync,
        op_kwargs={
            'config_path': '/opt/airflow/config/snowflake.json',
            'business_path': "{{ ti.xcom_pull(task_ids='fetch_yelp_updates')['business_path'] }}",
            'review_paths': "{{ ti.xcom_pull(task_ids='fetch_yelp_updates')['review_paths'] }}",
            'sentiment_path': "{{ ti.xcom_pull(task_ids='update_sentiment')['sentiment_path'] }}",
        }
    )
    
    # Define task dependencies
    fetch_updates_task >> update_sentiment_task >> update_snowflake_task