up:
	airflow webserver -D
	airflow scheduler -D
	airflow celery worker

up_single:
	airflow webserver -D
	airflow scheduler -D

down:
	sh ./down.sh

restart:
	make down 
	make up

init:
	airflow db migrate

jupyter:
	jupyter lab --no-browser --port=9999 --ip 0.0.0.0 --notebook-dir=.. --NotebookApp.token=''

suspend:
	sudo systemctl start suspend.target
