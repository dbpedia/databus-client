# CLI

## Installation
```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

## Execution example
```
bin/DatabusClient -s ./src/resources/queries/example.sparql -f jsonld -c gz
```

### CLI options

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
