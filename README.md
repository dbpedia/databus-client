# DBpedia Databus Client [![Build Status](https://travis-ci.org/dbpedia/databus-client.svg?branch=master)](https://travis-ci.org/dbpedia/databus-client)

Download and make data fit for applications using SPARQL on the [databus](https://databus.dbpedia.org).
 
## Vision
Any data on the bus can be made interoperable with application requirements. If the application can only read `.gz`, but not `.bz2` and only RDF-NTriples, but not RDF-XML, the client provides  `Download-As` functionality and transforms the data client-side. Files published on the Databus do not need to be offered in several formats. 

Example Application Deployment: Download the files of 5 datasets as given in the SPARQL query, transform all to `.bz2`, convert all RDF to RDF-NTriples and a) map the `.tsv` file from the second dataset to RDF with this <databus-uri> RML-Mapping, and b) use this <databus-uri> XSLT-Mapping for the `.xml` file in the fifth dataset. When finished, load and deploy a Virtuoso SPRARQL Endpoint via Docker. 

## Current State

**alpha**: 
code should compile and - if you are lucky - produce results for compression and RDF conversion. Please expect a tremendous amount of code refactoring and fluctuation. There will be an open-source licence, presumably GPL. 


## Concept

The databus-client is designed to unify and convert data on the client-side in several layers:

| Level | Client Action | Implemented formats
|---|---|---|
| 1 |  Download As-Is | All files on the [databus](https://databus.dbpedia.org)
| 2 |  Unify compression | bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate (, no compression)
| 3 |  Unify isomporphic formats | `Download as` implemented for {nt, ttl, rdfxml, json-ld} 
| 4 |  Transform with mappings | coming soon

### Roadmap for levels
* Level 1: all features finished, testing required
* Level 2: using Apache Compress library covers most of the compression formats, more testing required
* Level 3: Scalable RDF libraries from [SANSA-Stack](http://sansa-stack.net/) and [Databus Derive](https://github.com/dbpedia/databus-derive). Step by step, extension for all (quasi-)isomorphic [IANA mediatypes](https://www.iana.org/assignments/media-types/media-types.xhtml).
* Level 4:  In addition, we plan to provide a plugin mechanism to incorporate more sophisticated mapping engines as [RML](http://rml.io), R2RML, (R2R)[http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/] (for owl:equivalence translation) and XSLT. 


## CLI Example: Download the DBpedia ontology as RDF-XML
Ontology snapshots are uploaded to the Databus under [Denis Account](https://databus.dbpedia.org/denis/ontology/dbo-snapshots) (moved to DBpedia soon)


```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install

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

# Here is the script to download the latest ontology snapshot as ttl

bin/DownloadConverter --query ./latest_ontology.query --destination converted_files/ -f ttl

```

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
}" > query

# start docker as deamon by adding -d
docker run --name vosdc \
    -v $(pwd)/query:/opt/databus-client/query \
    -v $(pwd)/data:/data \
    -e QUERY="/opt/databus-client/query" \
    -p 8890:8890 \
    vosdc
```    

Container needs some startup time and endpoint is not immediately reachable, if it is done you can query it with e.g.

```
curl --data-urlencode query="SELECT * {<http://de.dbpedia.org/resource/Karlsruhe> ?p ?o }" "http://localhost:8890/sparql"
```



## Usage   

Installation
```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

Execution example
```
bin/DownloadConverter --query ./src/query/query1 --destination converted_files/ -f jsonld -c gz 
```

List of possible command line options.

| Option  | Description  | Default |
|---|---|---|
| -c, --compression  <arg> | set the compression format of the output file | `no compression`
| -d, --destination  <arg>| set the destination directory for converted files | `./converted_files/` |
| -f, --format  <arg> | set the file format of the output file  | `same` |  
| -q, --query  <arg> | any ?file query; You can pass the query directly or save it in a text file and pass the file path  | `/src/query/query` | 
| -s, --source  <arg>| set the source directory for files you want to convert| `./temp_dir_downloaded_files/` |
| --help| Show this message ||

You can load any ?file query. 
* You have the choice either to pass the query directly as a program variable (`-e QUERY="..."`), or save a query in a file and pass the filepath as variable.

<!---You can choose between different compression formats:
    
 * `bz2, gz, br, snappy-framed, deflate, lzma, xz, zstd` 

> **Important:** At the moment only conversion to NTriples(_"nt"_), TSV(_"tsv"_), Json-LD(_"jsonld"_) or _"same"_ possible
-->

### Single Modules

You can also use the converter and downloader separately.

**Databus based downloader**

```
bin/Downloader -q ./src/query/query1 -d ./downloaded_files/
```

**File compression and format converter**

```
bin/Converter --source ./src/resources/databus-client-testbed/format-testbed/2019.08.30/ -d ./converted_files/ -f rdfxml -c gz
```

## Dockerized Databus-Client

Creates a repo folder in the current directory, executes the query and loads resulting files into it.

```
mkdir repo
cd repo

echo "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcat:  <http://www.w3.org/ns/dcat#>

SELECT DISTINCT ?file  WHERE {
    ?dataset dataid:version <https://databus.dbpedia.org/marvin/mappings/geo-coordinates-mappingbased/2019.09.01> .
    ?dataset dcat:distribution ?distribution .
    ?distribution dcat:downloadURL ?file .
    ?distribution dataid:contentVariant ?cv .
     FILTER ( str(?cv) = 'de' )
}" > query

docker run --name databus-client \
    -v $(pwd)/query:/opt/databus-client/query \
    -v $(pwd):/var/repo \
    -e FORMAT="ttl" \
    -e COMPRESSION="bz2" \
    dbpedia/databus-client

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


You can pass all the variables as Environment Variables (**-e**), that are shown in the list above (except `destination` and `source`), but you have to write the Environment Variables in Capital Letters.


