# CLI Example

Download the `DBpedia ontology` (latest ontology snapshot) as `.ttl` without compression.

**Note**: Ontology snapshots are uploaded to the Databus under [Denis Account](https://databus.dbpedia.org/denis/ontology/dbo-snapshots) (moved to DBpedia soon)

______

#### Download and install DBpedia Databus Client
```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

#### Create Query file
```
echo "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
PREFIX dataid-cv: <http://dataid.dbpedia.org/ns/cv#>
PREFIX dataid-mt: <http://dataid.dbpedia.org/ns/mt#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat:  <http://www.w3.org/ns/dcat#>

# Get latest ontology NTriples file
SELECT DISTINCT ?file WHERE {
 	?dataset dataid:artifact <https://databus.dbpedia.org/denis/ontology/dbo-snapshots> .
	?dataset dcat:distribution ?distribution .
        ?distribution dcat:mediaType dataid-mt:ApplicationNTriples .
	?distribution dct:hasVersion ?latestVersion .  
	?distribution dcat:downloadURL ?file .

	{
	SELECT (?version as ?latestVersion) WHERE {
		?dataset dataid:artifact <https://databus.dbpedia.org/denis/ontology/dbo-snapshots> .
		?dataset dct:hasVersion ?version .
	} ORDER BY DESC (?version) LIMIT 1
	}

} " > latest_ontology.query
```


#### Execute DBpedia Databus Client
```
bin/DatabusClient --source ./latest_ontology.query -f ttl -c ""
```
