FROM apache/airflow:2.3.4
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY creds.json /creds.json
USER root
RUN apt-get -y update && apt-get -y install git
USER airflow
