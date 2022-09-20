# Eawag SURF Distributed Airflow Instance

This is a repository for Eawag SURF distributed airflow instance, initially developed for the ALPLAKES project.
The `docker-compose.yml` is based on the default configuration provided for containerising [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

DAGS can be found in the `airflow/dags` folder.

For local development UI pages are available at the following:

- Airflow Webserver (Managing DAGS) [http://localhost:8080/](http://localhost:8080/admin/)
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

#### Define environmental variables
- Replace **airflow** in the command below with desired admin password.
- Replace **fernet** in the command below with the desired fernet key.
```console
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\n_AIRFLOW_WWW_USER_PASSWORD=airflow\nAIRFLOW__CORE__FERNET_KEY=fernet" > .env
```
Fernet key can be generated as follows:
```python
from cryptography.fernet import Fernet
fernet_key = Fernet.generate_key()
print(fernet_key.decode())
```
Check content of .env contains the aforementioned variables.
```console
cat .env
```

#### Launch containers (API Node)
```console 
docker-compose up airflow-init
```
```console 
docker-compose -f docker-compose.yml up -d --build 
```
#### Launch container (Simulation Worker)
```console 
docker-compose up airflow-worker-simulation
```
#### Launch containers (Dev)
```console 
docker-compose up --profile simulation --build
```

### 5. Add credentials (API node only)

Create `creds.json` using `creds-example.json` as a template and populate the values.
```console 
cp creds-example.json creds.json
vim creds.json
```
Then go to the user interface and upload the credentials `Admin > Variables > Choose File > Import Variables`.

### 6. Open Ports

#### API Node

In order to access the web user interfaces you need to open the following ports:

- 8080 (Airflow UI)
- 5555 (Flower UI)

In order to communicate with the remote workers you need to open the following ports:

- TBC

#### Simulation Worker

In order to communicate with the API Node you need to open the following ports:

- TBC

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




