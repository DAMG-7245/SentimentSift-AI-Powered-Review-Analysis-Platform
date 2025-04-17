# airflow/dags/cafe_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from tasks.data_fetch import get_boston_cafes, get_cafe_reviews
from tasks.data_process import run_data_processing
from tasks.sentiment_analysis import run_sentiment_analysis
from tasks.theme_analysis import run_topic_modeling
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


def fetch_cafe_data(api_key, location, categories, limit, output_dir):
    """
    获取咖啡店数据并返回格式化的结果
    """
    os.makedirs(output_dir, exist_ok=True)
    

    cafes_data = get_boston_cafes(limit=limit)
    
    if not cafes_data or "data" not in cafes_data:
        raise Exception("Failed to get cafe data")
    

    business_path = os.path.join(output_dir, "businesses.json")
    with open(business_path, 'w', encoding='utf-8') as f:
        import json
        json.dump(cafes_data, f, ensure_ascii=False, indent=2)
    
    review_paths = []
    cafes = cafes_data.get("data", [])
  
    for i, cafe in enumerate(cafes[:limit]):
        business_id = cafe.get("business_id")
        if not business_id:
            continue
        
        reviews_data = get_cafe_reviews(business_id, limit=100)
        
        if reviews_data:
            review_path = os.path.join(output_dir, f"reviews_{business_id.replace(':', '_')}.json")
            review_paths.append(review_path)
    

    all_reviews_path = os.path.join(output_dir, "reviews.json")
    
    
    return {
        'business_path': business_path,
        'reviews_path': all_reviews_path
    }

with DAG(
    'cafe_sentiment_pipeline',
    default_args=default_args,
    description='Cafe sentiment analysis pipeline',
    schedule_interval=timedelta(days=7),
    catchup=False
) as dag:
    
    # Task 1: Fetch data from API
    fetch_data_task = PythonOperator(
        task_id='fetch_cafe_data',
        python_callable=fetch_cafe_data,
        op_kwargs={
            'api_key': '{{ var.value.api_key }}',
            'location': 'Boston, MA',
            'categories': 'cafes',
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