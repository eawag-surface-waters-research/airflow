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
git clone git@github.com:eawag-surface-waters-research/airflow.git
mkdir -p filesystem
```

### 3. Configure Environment

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

#### Auto update repo

If you want to auto pull any changes from the git repository. Make `update.sh` executable and add it to the crontab.
The example below pulls the repo every 5 mins, starting on the hour (so multiple instances update at the same time). 

```console
chmod +x update.sh
crontab -e
```
```crontab
0-59/5 * * * * /home/eawag.wroot.emp-eaw.ch/runnalja/airflow/update.sh
```


### 4. Launch Services

#### Production Environment
Main
```console 
docker compose -f docker-compose.yml up -d --build 
```
Worker
```console 
docker compose -f docker-compose-worker.yml up -d --build 
```
***
The worker node defaults to looking for connections on the local docker network.
If the worker node is on a different machine from the main node then the `POSTGRES_ADDRESS` and `REDIS_ADDRESS` variables need to be set in the .env file.

For the current Eawag setup this is:
```yaml
POSTGRES_ADDRESS=eaw-alplakes2:5432
REDIS_ADDRESS=eaw-alplakes2:6379
```
The setup above is only valid for machines inside the Eawag network, in order to launch workers on machines outside the Eawag network, 
the ports 6370 and 5432 would need to be exposed on the main node and the variables adjusted to the new addresses.
***
#### Development Environment
```console 
docker compose -f docker-compose.yml --profile dev up --build
```

***
It's possible that there could be a port conflict when launching the development environment due to Postgres/ Redis running on the host machine.
In this case it is possible to change the ports being exposed on the docker containers to avoid local conflicts by setting the following parameters in the .env file.
Default values are 5432 for Postgres and 6379 for Redis. 
```yaml
POSTGRES_PORT=5431
REDIS_PORT=6378
```

***

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


