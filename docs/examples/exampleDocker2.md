## Docker example: Deploy a small dataset to docker SPARQL endpoint
Loading geocoordinates extracted from DE Wikipedia into Virtuoso and host it locally

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client/docker

docker build -t vosdc -f virtuoso-image/Dockerfile virtuoso-image/

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

# delete docker from previous runs
# docker rm vosdc

# start docker as deamon by adding -d
docker run --name vosdc \
    -v $(pwd)/query.sparql:/opt/databus-client/query.sparql \
    -v $(pwd)/data:/data \
    -e SOURCE="/opt/databus-client/query.sparql" \
    -p 8890:8890 \
    vosdc
```    

Container needs some startup time and endpoint is not immediately reachable, if it is done you can query it with e.g.

```
curl --data-urlencode query="SELECT * {<http://de.dbpedia.org/resource/Karlsruhe> ?p ?o }" "http://localhost:8890/sparql"
```