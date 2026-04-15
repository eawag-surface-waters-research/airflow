# Debian GNU/Linux 12 (bookworm) with Python3.12
FROM apache/airflow:2.11.2-python3.12
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
USER root
RUN apt-get -y update && apt-get -y install git sshpass ca-certificates curl \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
       | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get -y install docker-ce-cli
USER airflow
