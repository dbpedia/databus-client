## Download Queries

The most important environment variable of the Download-Client is the download query. 
With the query you can specify an accurate selection of datasets of the [DBpedia-Databus](https://databus.dbpedia.org/), the Download-Client needs to process.

### Possible queries
* You can load any query that selects the object of the predicate `dcat:downloadURL`  
* The name of the variable doesn't matter
  * Query should look like: `SELECT ?o WHERE { ?s dcat:downloadURL ?o}`

### How to pass queries

There are three different ways to pass a query to the download-client:
1. Pass the query-string directly as a program variable
2. Save the query in a file and pass the filepath as variable
   * query-file extension must match `.sparql` or `.query`
3. Collection URIs are supported, too. The Client gets the related Query itself.
   * e.g. `https://databus.dbpedia.org/jfrey/collections/id-management_links`



## Command Line Options

| Option                   | Description                                                                                                                                                                                                                                                                      | Default |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| -s, --source  <arg>      | Set the source you want to convert. A source can either be a `[file/directory]` to convert already existing files, or a `[query file/query string/collection URI]` to convert queried files. Notice that query files must have `.sparql`/`.query` as extension to be recognized. ||
| -t, --target  <arg>      | Set the target directory for converted files                                                                                                                                                                                                                                     | `./files/` |
| -c, --compression  <arg> | Set the compression format of the output file                                                                                                                                                                                                                                    | `same`
| -f, --format  <arg>      | Set the file format of the output file                                                                                                                                                                                                                                           | `same` |
| -m, --mapping <arg>      | Set the mapping file for format-conversion to different format equivalence class                                                                                                                                                                                                 |
| -d, --delimiter <arg>    | Set the delimiter (only necessary for some formats)                                                                                                                                                                                                                              | , |
| -q, --quotation <arg>    | Set the quotation (only necessary for some formats)                                                                                                                                                                                                                              | " |
| --createMapping <arg>    | Do you want to create mapping files for mapped sources?                                                                                                                                                                                                                          | false |
| -g, --graphURI <arg>     | Set the graph uri for mapping from rdf triples to rdf quads                                                                                                                                                                                                                      |
| -b, --baseURI <arg>      | set the base URI to resolve relative URIs                                                                                                                                                                                                                                        |
| -o, --overwrite          | true -> overwrite files in cache, false -> use cache                                                                                                                                                                                                                             | `true`
| --clear                  | true -> clear Cache                                                                                                                                                                                                                                                              | `false`
| --help                   | Show this message                                                                                                                                                                                                                                                                ||


<!---You can choose between different compression formats:

 * `bz2, gz, br, snappy-framed, deflate, lzma, xz, zstd`

-->
