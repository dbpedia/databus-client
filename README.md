---
description: >-
  DBpedia Databus Client is a download and convertion tool, that makes data from
  any DBpedia Databus fit for your applications.
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
* [**Discord**](https://discord.gg/fB8byAPP7e)**:** Don't hesitate to ask us, if you have any questions.

## Quickstart

### Requirements

**Java:** `JDK 8` or `JDK 11`\
**Maven:** `Apache Maven 3.3.9`

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

You will find more information if you set the flag`-h` or in [CLI usage](docs/usage/cli.md).&#x20;

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
