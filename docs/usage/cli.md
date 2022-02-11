## CLI options

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

## Download Queries

Various datasets are registered on the DBpedia Databus in the form of files.
A download query specifies an exact selection of these records of the [DBpedia Databus](https://databus.dbpedia.org/) to be processed by the Databus Client.
Therefore, the download query is one of, if not the most important parameter of the client.

### Possible queries

You can pass any query that selects the object of the predicate `dcat:downloadURL`, the name of the variable does not matter.
The query should look like.  
```SELECT ?o WHERE { ?s dcat:downloadURL ?o}```


### How to pass queries

There are three different ways to pass a query to the Databus Client:
1. Pass the query string directly as a parameter.
2. Save the query in a file and pass the file path as a parameter.
    * The file extension of the query file must be `.sparql` or `.query`.
3. Collection URIs are also supported. The client receives the associated query itself.
    * e.g. `https://databus.dbpedia.org/jfrey/collections/id-management_links`