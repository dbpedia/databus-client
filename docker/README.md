# Docker Images

## Dockerized Databus-Client

build

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client/docker
docker build -t databus-client -f databus-client/Dockerfile  databus-client/
```

run 

```
docker run --name databus-client \
    -v /path/to/query:/opt/databus-client/query \
    -v /path/to/repo/:/var/repo \
    -e QUERY="/opt/databus-client/query" \
    databus-client
```

## Preloaded Virtuoso

Setup of a dockerized virtuoso and databus-client to preload the DB by a query.

### Docker-Compose (recommended)

> How to install [docker-compose](https://docs.docker.com/compose/install/) 

Start docker-compose including dockerized virtuoso and databus-client

```
git clone https://github.com/dbpedia/databus-client.git
cd docker
docker-compose -f virtuoso-compose/docker-compose.yml up 
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

### Dockerfile

```
git clone https://github.com/dbpedia/databus-client.git
cd docker
docker-compose -f databus-client/Dockerfile ./ 
```
