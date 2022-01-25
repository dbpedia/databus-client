## Dockerized Databus-Client

Creates a repo folder in the current directory, executes the query and loads resulting files into it.

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

Stopping and reseting the docker with name `databus-client`, e.g. to change the query

```
docker rm -f databus-client
```

Delete pulled image

```
docker rmi -f dbpedia/databus-client
```

&nbsp;


You can pass all the variables as Environment Variables (**-e**), that are shown in the list above (except `target`), but you have to write the Environment Variables in Capital Letters.