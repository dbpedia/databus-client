# CLI

## DBpedia Databus Client

### Installation

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

### Execution example

```
bin/DatabusClient -s ./src/resources/queries/example.sparql -f jsonld -c gz
```

You will find more information if you set the flag`-h` or in [CLI usage](cli.md).

## Separate downloader or converter

The converter and downloader of the Databus Client can be used separately.

### Databus based downloader

Since the parameters `compression` and `format` both have the default value `equal`, the Databus Client is a pure downloader if you do not pass any arguments for compression and format.

```
bin/DatabusClient -s ./src/resources/queries/example.sparql
```

### Compression and format converter

If you select already existing files as the `source`, the client does not use the download module and behaves like a pure converter.

```
bin/DatabusClient -s ./src/test/resources/databus-client-testbed/format-testbed/2019.08.30/ -f ttl -c gz
```

## CLI options

| Option            | Description                                                                                                                                                                                                                                                                      | Default    |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| -s, --source      | Set the source you want to convert. A source can either be a `[file/directory]` to convert already existing files, or a `[query file/query string/collection URI]` to convert queried files. Notice that query files must have `.sparql`/`.query` as extension to be recognized. |            |
| -t, --target      | Set the target directory for converted files                                                                                                                                                                                                                                     | `./files/` |
| -c, --compression | Set the compression format of the output file                                                                                                                                                                                                                                    | `same`     |
| -f, --format      | Set the file format of the output file                                                                                                                                                                                                                                           | `same`     |
| -m, --mapping     | Set the mapping file for format-conversion to different format equivalence class                                                                                                                                                                                                 |            |
| -d, --delimiter   | Set the delimiter (only necessary for some formats)                                                                                                                                                                                                                              | ,          |
| -q, --quotation   | Set the quotation (only necessary for some formats)                                                                                                                                                                                                                              | "          |
| --createMapping   | Do you want to create mapping files for mapped sources?                                                                                                                                                                                                                          | false      |
| -g, --graphURI    | Set the graph uri for mapping from rdf triples to rdf quads                                                                                                                                                                                                                      |            |
| -b, --baseURI     | set the base URI to resolve relative URIs                                                                                                                                                                                                                                        |            |
| -o, --overwrite   | true -> overwrite files in cache, false -> use cache                                                                                                                                                                                                                             | `true`     |
| --clear           | true -> clear Cache                                                                                                                                                                                                                                                              | `false`    |
| --help            | Show this message                                                                                                                                                                                                                                                                |            |

## Download Queries

Various datasets are registered on the DBpedia Databus in the form of files. A download query specifies an exact selection of these records of the [DBpedia Databus](https://databus.dbpedia.org/) to be processed by the Databus Client. Therefore, the download query is one of, if not the most important parameter of the client.

### Possible queries

You can pass any query that selects the object of the predicate `dcat:downloadURL`, the name of the variable does not matter. The query should look like.\
`SELECT ?o WHERE { ?s dcat:downloadURL ?o}`

### How to pass queries

There are three different ways to pass a query to the Databus Client:

1. Pass the query string directly as a parameter.
2. Save the query in a file and pass the file path as a parameter.
   * The file extension of the query file must be `.sparql` or `.query`.
3. Collection URIs are also supported. The client receives the associated query itself.
   * e.g. `https://databus.dbpedia.org/jfrey/collections/id-management_links`
