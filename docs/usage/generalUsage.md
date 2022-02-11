# General

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