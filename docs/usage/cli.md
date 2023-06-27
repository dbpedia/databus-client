# CLI

Instead of using the .jar you can also use Maven to execute the Databus Client.

### Requirements

* **Java:** `JDK 8` or `JDK 11`
* Maven: `^3.6`

### Installation

```
git clone https://github.com/dbpedia/databus-client.git
cd databus-client
mvn clean install
```

### Execution example

#### Select Query

First we need to specify, the data we want to download.

**Note:** _It is best practice to write a query to a file and pass this file as source, instead of passing the query directly as a string._

<pre><code><strong>echo "PREFIX dcat:   &#x3C;http://www.w3.org/ns/dcat#>
</strong>PREFIX databus: &#x3C;https://dataid.dbpedia.org/databus#>

SELECT ?file WHERE
{
        GRAPH ?g
        {
                ?dataset databus:artifact &#x3C;https://dev.databus.dbpedia.org/tester/testgroup/testartifact> .
                { ?distribution &#x3C;http://purl.org/dc/terms/hasVersion> '2023-06-23' . }
                ?dataset dcat:distribution ?distribution .
                ?distribution databus:file ?file .
        }
}" > query.sparql 
</code></pre>

#### Execute Client

```
bin/DatabusClient \
-s query.sparql \
-e https://dev.databus.dbpedia.org/sparql
-f jsonld \
-c gz
```

You will find more information if you set the flag`-h` or in [CLI usage](cli.md).

## Separate downloader or converter

The converter and downloader of the Databus Client can be used separately.

### Databus based downloader

Since the parameters `compression` and `format` both have the default value `equal`, the Databus Client is a pure downloader if you do not pass any arguments for compression and format.

```
bin/DatabusClient -s query.sparql
```

### Compression and format converter

If you select already existing files as the `source`, the client does not use the download module and behaves like a pure converter.

```
bin/DatabusClient -s query.sparql -f ttl -c gz
```

