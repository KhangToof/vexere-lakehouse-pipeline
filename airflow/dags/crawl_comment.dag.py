# Importing necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
# from crawl.crawl_reviews import crwl_reviews  #type: ignore
from crawl.crawl_reviews_new import crwl_reviews  #type: ignore
from crawl.crawl_ticket import crwl_ticket
from datetime import datetime, timedelta

# Defining the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 6),
}

# Defining the DAG
with DAG(
    'crawl_reviews',  # DAG ID
    default_args=default_args,
    description='crawling reviews from vexere.com',
    schedule='5 1 * * *',  # Cron expression for 3:05 AM every monday
    catchup=False  # Prevents backfilling of past dates
) as dag:

    # Defining the tasks
    # start_task = PythonOperator(
    #     task_id='start',  # Task ID
    #     python_callable=my_python_task,
    # )

    crawl_task = PythonOperator(
        task_id='crawl_reviews_task',
        python_callable=crwl_reviews,  # Function to execute
        dag=dag
    )

    # end_task = PythonOperator(
    #     task_id='end',
    #     python_callable=my_python_task,
    # )

    # Setting task dependencies
    crawl_task
