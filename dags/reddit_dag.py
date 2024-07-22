import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

'''
If you are running Docker and using Airflow in your Docker instance, you might encounter an error
 if you start your DAG without properly setting the root directory. 
 This is a common and well-known issue. 
 Ensure that you have configured the root directory correctly to avoid this error.
'''
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': 'Sneh Pillai',
    'start_date': datetime(2024, 7, 20)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'music_{file_postfix}',
        'subreddit': 'music',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

extract >> upload_s3