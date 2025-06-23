# Importing necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from crawl.crawl_ticket import crwl_ticket  #type: ignore
from convert.to_brz import convert  #type: ignore
from convert.to_silver import ticket_to_silver  #type: ignore
from datetime import datetime, timedelta

# Defining the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2025, 4, 3),
}

# Defining the DAG
with DAG(
    'crawl_ticket',  # DAG ID
    default_args=default_args,
    description='crawl n put thru delta n push',
    schedule='5 0 * * *',  # Cron expression for 12:05 AM every day
    catchup=False  # Prevents backfilling of past dates
) as dag:

    start_task = PythonOperator(
        task_id='crawl_data',
        python_callable=crwl_ticket,
        dag=dag
    )

    convert_task = PythonOperator(
    	task_id='convert_data',
        python_callable=convert,
        op_kwargs={'file_type': 'ticket'},
        dag=dag
     )

    to_sil = PythonOperator(
        task_id='push_data_to_silver',
        python_callable=ticket_to_silver,
        dag=dag
    )

    # Setting task dependencies
    start_task >> convert_task >> to_sil
