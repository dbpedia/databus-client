# Docker Images

old documentation. please refer to https://dbpedia.gitbook.io/databus/v/download-client
## Dockerized Databus-Client

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client/docker

docker build -t databus-client -f databus-client/Dockerfile  databus-client/

echo "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat:  <http://www.w3.org/ns/dcat#>

SELECT DISTINCT ?file  WHERE {
    ?dataset dataid:version <https://databus.dbpedia.org/marvin/mappings/geo-coordinates-mappingbased/2019.09.01> .
    ?dataset dcat:distribution ?distribution .
    ?distribution dcat:downloadURL ?file .
    ?distribution dataid:contentVariant ?cv .
     FILTER ( str(?cv) = 'de' )
}" > query.sparql

docker run --name databus-client \
    -v $(pwd)/query.sparql:/opt/databus-client/query.sparql \
    -v $(pwd)/repo:/var/repo \
    -e FORMAT="ttl" \
    -e COMPRESSION="bz2" \
    databus-client

docker rm databus-client
```

## Preloaded Virtuoso

Setup of a dockerized virtuoso and databus-client to preload the DB by a query.

### Single Dockerfile (recommended)

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client/docker

docker build -t vosdc -f virtuoso-image/Dockerfile virtuoso-image/

docker run --name vosdc \
    -v $(pwd)/databus-client/example.query:/opt/databus-client/example.query \
    -v $(pwd)/data:/data \
    -e SOURCE="/opt/databus-client/example.query" \
    -p 8890:8890 \
    vosdc
```

### Docker-Compose 

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
      - SOURCE="/opt/databus-client/example.query"
      - COMPRESSION="gz"
      - TARGET="/var/repo"
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


