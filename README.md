# DBpedia Databus Client

Collection of useful tools to download and convert data from the [databus](https://databus.dbpedia.org)

## Concept

The databus-client is designed to convert data in several layers:

| Level | Supported formats
|---|---|
| 1.  Download as it is | All files on the [databus](https://databus.dbpedia.org)
| 2.  Convert compression | bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate (, no compression)
| 3.  Convert format | nt, ttl, rdfxml, json-ld, tsv(write-only)
| 4.  Mapping | coming soon

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
