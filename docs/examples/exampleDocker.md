# Loading data into Virtuoso (Docker)

Deploy a dataset into a Docker SPARQL endpoint (Virtuoso).

### Requirements

* **Docker:** `^3.5`

## Execution

### Get docker-compose.yml

get the [docker-compose.yml](../../docker/virtuoso-compose/docker-compose.yml) file of the Databus Client Repository, or create your own:

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
      && rm -f /data/toLoad/complete && bash /virtuoso.sh'

  # To change the file query: Mount an external query
  # file under volumes between host and container
  # and apply internal path as environment variable.

  databus_client:
    image: dbpedia/databus-client:latest
    environment:
      - SOURCE=/databus-client/query.sparql
      - ENDPOINT=https://dev.databus.dbpedia.org/sparql
      - COMPRESSION=gz
    volumes:
      - ./myQuery.sparql:/databus-client/query.sparql
      - toLoad:/var/toLoad
    entrypoint: >
      bash -c 'bash /databus-client/entrypoint.sh
      && mv -t /var/toLoad $$(find /var/repo -name "*.gz");
      touch /var/toLoad/complete'

volumes:
  toLoad:
```

### Select your desired data

Again you need to specify your desired data in a sparql query

```
echo "PREFIX dcat:   <http://www.w3.org/ns/dcat#>
PREFIX databus: <https://dataid.dbpedia.org/databus#>

SELECT ?file WHERE
{
        GRAPH ?g
        {
                ?dataset databus:artifact <https://dev.databus.dbpedia.org/tester/testgroup/testartifact> .
                { ?distribution <http://purl.org/dc/terms/hasVersion> '2023-06-23' . }
                ?dataset dcat:distribution ?distribution .
                ?distribution databus:file ?file .
        }
}" > myQuery.sparql
```

### Start Containers

```
docker compose up
```

Container needs some startup time and endpoint is not immediately reachable. If it is done you can query it with directly in your browser at [http://localhost:8895/sparql/](http://localhost:8895/sparql/) or you can query directly in your terminal: e.g.

```
curl --data-urlencode query="SELECT * {?a <http://xmlns.com/foaf/0.1/account> ?o }" "http://localhost:8895/sparql"
```

#### Useful commands

Stopping and reseting the docker with name `databus-client`, e.g. to change the query

```
docker rm -f databus-client
```

Delete pulled image

```
docker rmi -f dbpedia/databus-client
```
