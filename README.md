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

### 3. Add credentials

Create `creds.json` using `creds-example.json` as a template and populate the values.
```console 
cp creds-example.json creds.json
vim creds.json
```

### 4. Launch containers

#### API Server
```console 
docker-compose -f docker-compose-api.yml up -d --build 
```
#### Simulation Server
```console 
docker-compose -f docker-compose-simulation.yml up -d --build 
```

## Docker Commands

### Terminate containers
#### API Server
```console 
docker-compose -f docker-compose-api.yml down
```
#### Simulation Server
```console 
docker-compose -f docker-compose-simulation.yml down
```

### List active containers
```console 
docker ps
```

### Go inside the container
```console 
docker exec -it 'container-id' bash
```




