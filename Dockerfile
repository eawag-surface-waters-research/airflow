# Debian GNU/Linux 11 (bullseye) with Python3.9
FROM apache/airflow:2.3.4-python3.9
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
USER root
RUN apt-get -y update && apt-get -y install git
RUN curl https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-ce-cli_20.10.18~3-0~debian-bullseye_amd64.deb  --output /docker-ce-cli_20.10.18~3-0~debian-bullseye_amd64.deb
RUN dpkg -i /docker-ce-cli_20.10.18~3-0~debian-bullseye_amd64.deb
USER airflow
