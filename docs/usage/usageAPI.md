# Usage (API)

The DBpedia Databus-Client offers an API for easy integration into your project.

### Example-Code
```
DatabusClient
    .source("./src/query/query1")
    .source("./src/query/query2")
    .compression(Compression.gz)
    .format(Format.nt)
    .execute()
```

### Example-Project
We have also created a [Sample-Project](https://github.com/Eisenbahnplatte/Databus-Client-Example) that shows how the Databus-Client can be integrated into a project.
