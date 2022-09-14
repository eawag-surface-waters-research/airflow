# Eawag Distributed Airflow Instance

This is a repository for Eawag's distributed airflow instance, initially developed for the ALPLAKES project.

DAGS can be found in the `airflow/dags` folder. 

For local development UI pages are available at the following:

- Airflow Webserver (Managing DAGS) [http://localhost:8080/admin/](http://localhost:8080/admin/)
- Flower Workers (Managing Workers) [http://localhost:5555/](http://localhost:5555/)

## Install docker-compose
```console 
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
console sudo chmod +x /usr/local/bin/docker-compose
console sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
```

## Docker Commands

### Launch containers

#### API Server
```console 
docker-compose -f docker-compose-api.yml up -d --build 
```
#### Simulation Server
```console 
docker-compose -f docker-compose-simulation.yml up -d --build 
```

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




