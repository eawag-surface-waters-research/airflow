# Eawag SURF Distributed Airflow Instance

This is a repository for Eawag SURF distributed airflow instance, initially developed for the ALPLAKES project.

DAGS can be found in the `airflow/dags` folder. 

For local development UI pages are available at the following:

- Airflow Webserver (Managing DAGS) [http://localhost:8080/admin/](http://localhost:8080/admin/)
- Flower Workers (Managing Workers) [http://localhost:5555/](http://localhost:5555/)

## Installation

### 1. Install docker-compose
```console 
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
console sudo chmod +x /usr/local/bin/docker-compose
console sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
```
### 2. Clone repository
```console 
sudo apt-get -y update && sudo apt-get -y install git
git clone https://github.com/eawag-surface-waters-research/airflow.git
```

### 3. Launch containers

#### Add credentials

Create `creds.json` using `creds-example.json` as a template and populate the values.
```console 
cp creds-example.json creds.json
vim creds.json
```

#### Setting the Airflow user & admin password
Replace **airflow** in the command below with desired admin password.
```console
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\n_AIRFLOW_WWW_USER_PASSWORD=airflow" > .env
```
Check content of .env contains the three aforementioned variables.
```console
cat .env
```

#### Launch database
```console 
docker-compose up airflow-init
```
```console 
docker-compose -f docker-compose.yml up -d --build 
```

## Docker Commands

### Terminate containers
```console 
docker-compose -f docker-compose.yml down
```

### List active containers
```console 
docker ps
```

### Go inside the container
```console 
docker exec -it 'container-id' bash
```




