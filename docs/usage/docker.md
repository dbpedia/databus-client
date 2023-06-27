# Docker

We also provide a dockerized version of the databus client in our [DockerHub repository](https://hub.docker.com/r/dbpedia/databus-client).

You can pass all the variables as Environment Variables (**-e**), that are shown in [CLI options](broken-reference) (except `target`), but you have to write the Environment Variables in Capital Letters.

### Requirements

* **Docker:** `^3.5`

### Installation

Pull a docker image of our [DockerHub repository](https://hub.docker.com/r/dbpedia/databus-client).

```
docker pull dbpedia/databus-client:latest
```

## Example

#### Select Data

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

```
docker run --name databus-client \
    -v $(pwd)/myQuery.sparql:/databus-client/query.sparql \
    -v $(pwd)/repo:/var/repo \
    -e ENDPOINT="https://dev.databus.dbpedia.org/sparql" \
    -e SOURCE="/databus-client/query.sparql" \
    -e FORMAT="ttl" \
    -e COMPRESSION="bz2" \
    dbpedia/databus-client
```
