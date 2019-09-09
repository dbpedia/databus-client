# DBpedia Databus Client [![Build Status](https://travis-ci.org/dbpedia/databus-client.svg?branch=master)](https://travis-ci.org/dbpedia/databus-client)

Download and make data fit for applications using SPARQL on the [databus](https://databus.dbpedia.org).
 
## Vision
Any data on the bus can be made interoperable with application requirements. If the application can only read `.gz`, but not `.bz2` and only RDF-NTriples, but not RDF-XML, the client provides  `Download-As` functionality and transforms the data client-side. Files published on the Databus do not need to be offered in several formats. 

Example Application Deployment: Download the files of 5 datasets as given in the SPARQL query, transform all to `.bz2`, convert all RDF to RDF-NTriples and a) map the `.tsv` file from the second dataset to RDF with this <databus-uri> RML-Mapping, and b) use this <databus-uri> XSLT-Mapping for the `.xml` file in the fifth dataset. When finished, load and deploy a Virtuoso SPRARQL Endpoint via Docker. 

## Current State

**working alpha**: 
code should compile and often produce correct results for compression and RDF conversion. Please expect a tremendous amount of code refactoring and fluctuation. There will be an open-source licence, presumably GPL. 


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

## Usage   

Installation
```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

Execution example
```
mvn scala:run -Dlauncher=downloadconverter --query ./src/query/downloadquery --targetrepo converted_files/ -c gz -f jsonld```
```

List of possible command line options.

| Option  | Description  | Default |
|---|---|---|
| -c, --compression  <arg> | set the compression format of the output file | `no compression`
| -d, --dest  <arg>| set the destination directory for converted files | `./converted_files/` |
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
mvn scala:run -Dlauncher=downloader -q ./src/query/downloadquery -t ./downloaded_files/```
```

**File compression and format converter**

```
mvn scala:run -Dlauncher=converter --src ./downloaded_files/ -t ./converted_files/ -c gz -f jsonld
```

## Docker

Build the docker image.

```
docker build -t databus-client ./ 
```

Run a docker container.

```
docker run -p 8890:8890 --name client -e LAUNCHER=downloadconverter -e QUERY=/root/dbpediaclient/src/query/query dbpedia-client
```

You have to specify the launcher you want the container to execute. Therefore you need to pass the name of the launcher as environment variable (`LAUNCHER` or `L`)
```
-e LAUNCHER=downloader
```

Additionally you can pass all the variables that are shown in the list above as Environment Variables (**-e**).  
You have to write the Environment Variables in Capital Letters, if you use docker to execute.  

```
docker run -p 8890:8890 --name client -e L=downloadconverter -e Q=<path> -e F=nt -e C=gz dbpedia-client
```

To stop the image *client* in the container *dbpedia-client* use `docker stop client`

> **Important:** If you use docker to execute, you can't change the "_TARGETREPO_" yet.
