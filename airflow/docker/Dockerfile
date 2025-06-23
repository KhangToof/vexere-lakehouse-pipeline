ARG AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5-python3.11"
FROM ${AIRFLOW_IMAGE_NAME}

ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

USER root
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install -y python3-pip

COPY airflow_requirements.txt .

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r airflow_requirements.txt

COPY scripts scripts
RUN chmod +x scripts

USER 1000