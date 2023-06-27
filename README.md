---
description: >-
  Download data from DBpedia Databus using SPARQL and make it data fit for your
  applications.
---

# Overview

The DBpedia Databus Client simplifies data consumption and compilation from the DBpedia Databus, addressing challenges in using data from different publishers and domains.&#x20;

Data is often released in various serialization and compression formats, requiring conversion before it can be utilized. Additionally, tabular structured data like in relational databases or data from community-specific formats necessitates mapping for integration with knowledge graphs. Currently, mapping efforts are dispersed, leading to reduced reusability and unclear provenance.&#x20;

To address these issues, we propose a client that can automatically convert and compile data assets registered on any DBpedia Databus distribution into formats supported by the target infrastructure. It enables seamless consumption of compiled data, similar to traditional software dependency management systems. By shifting the burden of format conversion from data providers to the client, we reduce the publishing effort, enhance data consumption with fewer conversion problems, enable data-driven applications with automatically updated dependencies and enhance the findability and reuse of mapping definitions.

The client brings us closer to realizing a unified and efficient data ecosystem, promoting reusability and maintaining clear provenance.

## Status

**Beta**: The Databus Client produces expected results for compression conversion and file format conversion. Errors could occure for the mapping process. Please expect some code refactoring and fluctuation.

## Important Links

* [**Documentation**](https://dbpedia.gitbook.io/databus/v/download-client/)
* [**Source Code**](https://github.com/dbpedia/databus-client/tree/master)
* [**Latest Release**](https://github.com/dbpedia/databus-client/releases/latest)
* [**Discord**](https://discord.gg/fB8byAPP7e)**:** Don't hesitate to ask, if you have any questions.

## Quickstart

### Requirements

You have multiple options to run the client (shown in [usage](docs/usage/ "mention")). For the standalone approach (.jar file) you only need `Java`installed on your machine.

* **Java:** `JDK 8` or `JDK 11`

### Installation

Download `databus-client.jar` of the latest [Databus Client release](https://github.com/dbpedia/databus-client/releases/latest).

### Choose Data for your application

First, we need to select the Databus, where we want to get our data from. We take [this](https://dev.databus.dbpedia.org/) databus. Its SPARQL Endpoint is located here: [https://dev.databus.dbpedia.org/sparql](https://dev.databus.dbpedia.org/sparql) .

To select data from a DBpedia Databus, you can perform queries. Databus provides two mechanisms for this, which are described in detail [here](https://dbpedia.gitbook.io/databus/#querying-metainformation).&#x20;

We use the following query as selection for this example:

```
PREFIX dcat:   <http://www.w3.org/ns/dcat#>
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
}
```

### Download and convert selected data

In order to download the data we need to pass the query as the _`-s`_ argument. Additionaly we need to specify where the query needs to be asked to. This is done using the `-e` argument. Furthermore if we want to convert the files to _.nt_ we need to specify if in the _`-f`_ parameter and finally we need to tell the client the desired compression. There are more options described in [#cli-options](docs/usage/cli.md#cli-options "mention")

```
java -jar target/databus-client-v2.1-beta.jar \
-s "PREFIX dcat:   <http://www.w3.org/ns/dcat#>
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
}" \
-e "https://dev.databus.dbpedia.org/sparql" \
-f nt
```

Per default the resulting files will be saved to `./files/` . &#x20;

## Contributing

Please report issues in our [github repository](https://github.com/dbpedia/databus-client).

If you would like to submit a non-trivial patch or pull request we will need you to sign the Contributor License Agreement, we will send it to you in that case.

## License

The source code of this repo is published under the [Apache License Version 2.0](https://github.com/AKSW/jena-sparql-api/blob/master/LICENSE)

Databus is configured so that the default license of all metadata is CC-0, which is relevant for all data of the Model, i.e. who published which data, when and under which license.

The individual datasets are referenced via links (dcat:downloadURL) and can have any license.

## Citation

If you use the DBpedia Databus Client in your research, please cite the following paper:

```bibtex
@InProceedings{mcdc2021,
  author = {Johannes Frey and Fabian G\"otz and Marvin Hofer and Sebastian Hellmann},
  title = {Managing and Compiling Data Dependencies for Semantic Applications using Databus Client},
  booktitle = {Metadata and Semantic Research. MTSR 2021}
  year = {2021},
}
```
