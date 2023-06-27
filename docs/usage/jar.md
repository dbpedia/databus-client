# JAR

Instead of cloning the whole repository, you can only download the `databus-client.jar` of the latest [Databus Client release](https://github.com/dbpedia/databus-client/releases/latest).&#x20;

The parameter options are shown in [#databus-client-parameters](./#databus-client-parameters "mention")

### Requirements

* **Java:** `JDK 11`

## Execution example

### Select Data

First we need to specify, the data we want to download.

**Note:** _It is best practice to write a query to a file and pass this file as source, instead of passing the query directly as a string._

```
echo "PREFIX dcat:   <http://www.w3.org/ns/dcat#>
PREFIX databus: <https://dataid.dbpedia.org/databus#>

SELECT ?file WHERE
{
        GRAPH ?g
        {
                ?dataset databus:artifact <https://dev.databus.dbpedia.org/tester/testgroup/testartifact> .
                { ?distribution <http://purl.org/dc/terms/hasVersion> '2023-06-23' . }
                ?dataset dcat:distribution ?distribution .
                ?distribution databus:file ?file .
        }
}" > query.sparql
```

### Download and Convert Data

Then we can download the selected data, and convert it to ntriple files.

```
java -jar databus-client.jar \
-s "query.sparql" \
-e "https://dev.databus.dbpedia.org/sparql" \
-f nt
```
