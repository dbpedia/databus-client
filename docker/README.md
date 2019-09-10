# Docker Images

## Dockerized Databus-Client

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client/docker
docker build -t databus-client -f databus-client/Dockerfile  databus-client/
docker run --name databus-client \
    -v $(pwd)/databus-client/example.query:/opt/databus-client/query \
    -v $(pwd):/var/repo \
    -e QUERY="/opt/databus-client/query" \
    databus-client
```

## Preloaded Virtuoso

Setup of a dockerized virtuoso and databus-client to preload the DB by a query.

### Single Dockerfile (recommended)

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client/docker

docker build -t vosdc -f virtuoso-image/Dockerfile virtuoso-image/

docker run --name vosdc \
    -v $(pwd)/databus-client/example.query:/opt/databus-client/query \
    -v $(pwd)/data:/data \
    -e QUERY="/opt/databus-client/query" \
    -p 8890:8890 \
    vosdc
```

### Docker-Compose 

> How to install [docker-compose](https://docs.docker.com/compose/install/) 

Start docker-compose including dockerized virtuoso and databus-client

```
git clone https://github.com/dbpedia/databus-client.git
cd docker
docker-compose -f databus-client/Dockerfile ./ 
```

Change `docker/virtuoso-compose/docker-compose.yml` configuration if needed.

```
version: '3.5'

services:

  db:
    image: tenforce/virtuoso
    ports:
      - 8895:8890
    volumes:
      - toLoad:/data/toLoad
    entrypoint: >
      bash -c 'while [ ! -f /data/toLoad/complete ]; do sleep 1; done
      && bash /virtuoso.sh'

  databus_client:
    build: ../databus-client
    environment:
      - QUERY="/opt/databus-client/example.query"
      - COMPRESSION="gz"
      - DEST="/var/repo"
    volumes:
      - ../databus-client/example.query:/opt/databus-client/example.query
      - toLoad:/var/toLoad
    entrypoint: >
      bash -c 'bash /opt/databus-client/entrypoint.sh
      && mv -t /var/toLoad $$(find /var/repo -name "*.gz")
      && touch /var/toLoad/complete'

volumes:
  toLoad:
```


