from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from crawl.crawl_ticket import crwl_ticket  # type: ignore
from crawl.crawl_faci import crwl_facility  # type: ignore
from crawl.crawl_reviews_new import crwl_reviews  # type: ignore

from convert.to_brz import convert  # type: ignore
from convert.to_silver import ticket_to_silver, facility_to_silver, review_to_silver  # type: ignore
from convert.to_gold import update_charts # type: ignore

from audit.audit_logger import write_audit_logs  # type: ignore

from predict.sentiment_analysis import process_sentiment_analysis_en, process_sentiment_analysis_vi  # type: ignore

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2025, 5, 11),
}

with DAG(
    'khoa_luan_tot_nghiep_2025',
    default_args=default_args,
    description='Airflow DAG for ticket, facility, and review pipelines',
    schedule='5 0 * * *',
    catchup=False
) as dag:

    # Ticket pipeline group
    with TaskGroup("ticket_pipeline", tooltip="Crawl and process ticket data") as ticket_group:
        ticket_crawl = PythonOperator(
            task_id='crawl_tickets',
            python_callable=crwl_ticket
        )

        convert_ticket = PythonOperator(
            task_id='tickets_convert',
            python_callable=convert,
            op_kwargs={'file_type': 'ticket'}
        )

        ticket_to_silver_task = PythonOperator(
            task_id='push_tickets',
            python_callable=ticket_to_silver
        )

        ticket_crawl >> convert_ticket >> ticket_to_silver_task

    # Facility pipeline group
    with TaskGroup("facility_pipeline", tooltip="Crawl and process facility data") as facility_group:
        facility_crawl = PythonOperator(
            task_id='crawl_facilities',
            python_callable=crwl_facility
        )

        convert_facility = PythonOperator(
            task_id='facilities_convert',
            python_callable=convert,
            op_kwargs={'file_type': 'facility'}
        )

        facility_to_silver_task = PythonOperator(
            task_id='push_facilities',
            python_callable=facility_to_silver
        )

        facility_crawl >> convert_facility >> facility_to_silver_task

    # Review pipeline group
    with TaskGroup("review_pipeline", tooltip="Crawl, train and process review data") as review_group:
        review_crawl = PythonOperator(
            task_id='crawl_reviews',
            python_callable=crwl_reviews
        )

        convert_reviews_task = PythonOperator(
            task_id='reviews_convert',
            python_callable=convert,
            op_kwargs={'file_type': 'review'} #
        )

        train_vi_task = PythonOperator(
            task_id='train_vi',
            python_callable=process_sentiment_analysis_vi
        )

        train_en_task = PythonOperator(
            task_id='train_en',
            python_callable=process_sentiment_analysis_en
        )

        review_to_silver_task = PythonOperator(
            task_id='push_reviews',
            python_callable=review_to_silver
        )

        review_crawl >> convert_reviews_task
        convert_reviews_task >> [train_vi_task, train_en_task] >> review_to_silver_task

    update_charts = PythonOperator(
        task_id='update_charts',
        python_callable=update_charts,
    )

    audit_task = PythonOperator(
        task_id='write_audit_logs',
        python_callable=write_audit_logs,
        provide_context=True
    )

    # DAG dependency
    ticket_group >> facility_group >> review_group >> update_charts >> audit_task
