export WEBSERVER_PID=$(cat ~/airflow/airflow-webserver.pid)
export SCHEDULER_PID=$(cat ~/airflow/airflow-scheduler.pid)
kill -9 $WEBSERVER_PID
kill -9 $SCHEDULER_PID
