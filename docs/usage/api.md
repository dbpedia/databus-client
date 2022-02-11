# Quickstart - API

The Databus Client also offers an API for easy integration into your project.

## Example 
```
DatabusClient
    .source("./src/query/query1")
    .source("./src/query/query2")
    .compression(Compression.gz)
    .format(Format.nt)
    .execute()
```

## Sample project

We have also created a [sample project](https://github.com/dbpedia/databus-client/tree/master/examples/sample_project) that shows how the Databus Client can be integrated into a project.