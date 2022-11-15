# Eawag Workflow Automation

[![License: MIT][mit-by-shield]][mit-by] ![Python][python-by-shield]

This distributed, dockerized [Apache Airflow](https://airflow.apache.org) instance runs automated workflows in support of projects from the [Department Surface Waters](https://www.eawag.ch/en/department/surf) at Eawag. 
Workflows are defined as DAGS ([what is a DAG?](https://airflow.apache.org/docs/apache-airflow/1.10.10/concepts.html#dags)) and can be found in the `airflow/dags` folder. 

Example workflows are as follows:

- Downloading COSMO data from MeteoSwiss `airflow/dags/download_meteoswiss_cosmo.py`
- Downloading Hydrodata from BAFU `airflow/dags/download_bafu_hydrodata.py`
- Delft3D Simulation of Greifensee `airflow/dags/simulate_delft3dflow_operational_greifensee.py`

This is planned to be expanded to a number of projects in the department including operational runs of Sencast and Simstrat.

A web portal for managing the workflows is available at `http://eaw-alplakes2:8080` for users connected to the Eawag VPN.

![Apache Airflow][Airflow]

## Installation

### 1. Install docker

Follow the official install instructions [here](https://docs.docker.com/engine/install/)

Then run `docker login` to enable access to private docker images.

### 2. Clone repository
```console
sudo apt-get -y update && sudo apt-get -y install git
git clone https://github.com/eawag-surface-waters-research/airflow.git
mkdir -p filesystem
```

### 3. Launch containers

#### Define environmental variables
Copy the env.example file to .env and complete the required passwords.
```console
cd airflow
cp env.example .env
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

#### Add ssh keys

The `keys` folder will be mounted to the docker instance at `/opt/airflow/keys`.

Upload your keys to the server. There is often issues with permissions, suggested is `chmod -R 777 keys`, `chmod 700 keys/id_rsa`

#### Launch containers (Production)
```console 
docker compose -f docker-compose.yml up -d --build 
```
#### Launch containers (Local)
```console 
docker compose -f docker-compose.yml --profile simulation up --build
```

## Managing Instance

#### Terminate containers
```console 
docker compose -f docker-compose.yml down
```

#### List active containers
```console 
docker ps
```

#### Go inside the container
```console 
docker exec -it 'container-id' bash
```

## Adding Workflows

New workflows can be added by including new python dags into the `airflow/dags` folder. 
They will then be picked up automatically by the system. Best practice for adding new DAGS is as follows:

- Set up a local instance of airflow (see instructions above)
- Create a new branch `git checkout -b newbranchname`
- Add your new DAG and test it
- Commit your changes and create a pull request to the master branch

[mit-by]: https://opensource.org/licenses/MIT
[mit-by-shield]: https://img.shields.io/badge/License-MIT-g.svg
[python-by-shield]: https://img.shields.io/badge/Python-3.9-g
[airflow]: https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white


