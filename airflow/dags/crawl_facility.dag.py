# Importing necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from crawl.crawl_faci import crwl_facility  #type: ignore
from convert.to_brz import convert  #type: ignore
from convert.to_silver import facility_to_silver  #type: ignore
from datetime import datetime, timedelta

# Defining the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 30),
}

# Defining the DAG
with DAG(
    'crawl_facility',  # DAG ID
    default_args=default_args,
    description='crawling facilities from vexere.com',
    schedule='5 1 * * *',  # Cron expression for 1:05 AM every monday
    catchup=False  # Prevents backfilling of past dates
) as dag:

    # Defining the tasks
    # start_task = PythonOperator(
    #     task_id='start',  # Task ID
    #     python_callable=my_python_task,
    # )

    crawl_task = PythonOperator(
        task_id='crawl_facilities',
        python_callable=crwl_facility,  # Function to execute
        dag=dag
    )

    convert_task = PythonOperator(
       task_id='convert_data',
       python_callable=convert,
       op_kwargs={'file_type': 'facility'},
       dag=dag
    )

    to_sil = PythonOperator(
        task_id='push_data_to_silver',
        python_callable=facility_to_silver,
        dag=dag
    )

    # Setting task dependencies
    crawl_task >> convert_task >> to_sil
