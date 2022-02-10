## Download Queries

The download query is one of, if not the most important parameter of the Databus Client.
It specifies a precise selection of the records of the [DBpedia databus](https://databus.dbpedia.org/) that the download client should process.

### Possible queries
* Any query that selects the object of the predicate `dcat:downloadURL`
* Name of the variable doesn't matter
    * Query should look like: `SELECT ?o WHERE { ?s dcat:downloadURL ?o}`

### How to pass queries

There are three different ways to pass a query to the download-client:
1. Pass the query-string directly as a program variable
2. Save the query in a file and pass the filepath as variable
    * query-file extension must match `.sparql` or `.query`
3. Collection URIs are supported, too. The Client gets the related Query itself.
    * e.g. `https://databus.dbpedia.org/jfrey/collections/id-management_links`
