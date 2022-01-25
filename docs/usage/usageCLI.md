# Usage (CLI)

### Installation
```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

### Execution example
```
bin/DatabusClient --source ./src/resources/queries/example.sparql --target converted_files/ -f jsonld -c gz
```
____________________
You can also use the released jar, instead of cloning the whole repository
```
java -jar databus-client-1.0-SNAPSHOT.jar -s "https://databus.dbpedia.org/rogargon/collections/browsable_core"
```
______________
### List of possible command line options.

| Option  | Description  | Default |
|---|---|---|
| -s, --source  <arg> | Set the source you want to convert. A source can either be a `[file/directory]` to convert already existing files, or a `[query file/query string/collection URI]` to convert queried files. Notice that query files must have `.sparql`/`.query` as extension to be recognized.||
| -t, --target  <arg> | Set the target directory for converted files | `./files/` |
| -c, --compression  <arg> | Set the compression format of the output file | `same`
| -f, --format  <arg> | Set the file format of the output file  | `same` |
| -m, --mapping <arg> | Set the mapping file for format-conversion to different format equivalence class |
| -d, --delimiter <arg> | Set the delimiter (only necessary for some formats) | , |
| -q, --quotation <arg> | Set the quotation (only necessary for some formats) | " |
| --createMapping <arg> | Do you want to create mapping files for mapped sources? | false |
| -g, --graphURI <arg> | Set the graph uri for mapping from rdf triples to rdf quads |
| -o, --overwrite | true -> overwrite files in cache, false -> use cache | `true`
| --clear | true -> clear Cache | `false`
| --help| Show this message ||

You can load any query with one variable selected. That variable must be the object of the predicate `dcat:downloadURL`.    
So the query should look like: `SELECT ?o WHERE { ?s dcat:downloadURL ?o}`
* You have the choice either to pass the query directly as a program variable, or save it in a file and pass the filepath as variable. The query file name must match `*.sparql` or `*.query`.
* Additionally, Collection URIs are supported now (e.g. `https://databus.dbpedia.org/jfrey/collections/id-management_links`). The Client gets the related Query itself.

<!---You can choose between different compression formats:

 * `bz2, gz, br, snappy-framed, deflate, lzma, xz, zstd`

-->

