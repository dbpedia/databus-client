# Docker Compose

Whether you prefer to use docker or docker compose is a matter of taste. We provide both options.

### Requirements

* **Docker:** `^3.5`

## Example

#### Installation

get the [docker-compose.yml](../../docker/databus-client-compose/docker-compose.yml) file of the Databus Client Repository

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

#### Execute

```
docker compose up
```

\-> the resulting files can be found in the `toLoad` Volume
