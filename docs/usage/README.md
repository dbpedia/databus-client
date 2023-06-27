# Usage

## Databus Client Parameters

<table><thead><tr><th width="322.3333333333333">Option</th><th>Description</th><th>Default</th></tr></thead><tbody><tr><td>-s, --source</td><td>Set the source you want to convert. A source can either be a <code>[file/directory]</code> to convert already existing files, or a <code>[query file/query string/collection URI]</code> to convert queried files. Notice that query files must have <code>.sparql</code>/<code>.query</code> as extension to be recognized.</td><td></td></tr><tr><td>-e, --endpoint</td><td>Set the sparql endpoint, where the query should be fired to. If you use a collection you don't need this parameter, because its detected automatically. Otherwise its mandatory.</td><td></td></tr><tr><td>-t, --target</td><td>Set the target directory for converted files</td><td><code>./files/</code></td></tr><tr><td>-c, --compression</td><td>Set the compression format of the output file</td><td><code>same</code></td></tr><tr><td>-f, --format</td><td>Set the file format of the output file</td><td><code>same</code></td></tr><tr><td>-m, --mapping</td><td>Set the mapping file for format-conversion to different format equivalence class</td><td></td></tr><tr><td>-d, --delimiter</td><td>Set the delimiter (only necessary for some formats)</td><td>,</td></tr><tr><td>-q, --quotation</td><td>Set the quotation (only necessary for some formats)</td><td>"</td></tr><tr><td>--createMapping</td><td>Do you want to create mapping files for mapped sources?</td><td>false</td></tr><tr><td>-g, --graphURI</td><td>Set the graph uri for mapping from rdf triples to rdf quads</td><td></td></tr><tr><td>-b, --baseURI</td><td>set the base URI to resolve relative URIs</td><td></td></tr><tr><td>-o, --overwrite</td><td>true -> overwrite files in cache, false -> use cache</td><td><code>true</code></td></tr><tr><td>--clear</td><td>true -> clear Cache</td><td><code>false</code></td></tr><tr><td>--help</td><td>Show this message</td><td></td></tr></tbody></table>

## Queries

Various datasets are registered on the DBpedia Databus in the form of files. A query specifies an exact selection of these records of the [DBpedia Databus](https://databus.dbpedia.org/) to be processed by the Databus Client. Therefore, the query is one of, if not the most important parameter of the client.

### Possible queries

You can pass any query that selects the object of the predicate `databus:file`, the name of the variable does not matter. The query should look like.\
`SELECT ?o WHERE { ?s databus:file ?o}`

### How to pass queries

There are three different ways to pass a query to the Databus Client:

1. Pass the query string directly as a parameter.
   * **Note:** this option does not work for [cli.md](cli.md "mention")
2. Save the query in a file and pass the file path as a parameter.
   * The file extension of the query file must be `.sparql` or `.query`.
3. Collection URIs are also supported. The client receives the associated query itself.
   * e.g. [https://dev.databus.dbpedia.org/testuser/collections/testcollection/](https://dev.databus.dbpedia.org/testuser/collections/testcollection/)
