# Vision

Although data repositories or management platforms with rich homogeneous metadata catalogs like the DBpedia Databus allow to manage, find, and access files in a unified way, difficulties arise if consumers want to use data from different publishers and domains. These files can be released in various serialization formats (e.g. RDF can be represented in more than 8 formats) and compression variants, that typically can not be read all by an application or workflow without any prior conversion. Moreover, in many research disciplines, data is stored in relational databases and exported into tabular-structured data formats (e.g. CSV) or specialized community-specific formats. Loading this data alongside knowledge graphs requires a mapping process to be performed on the consumer side. However, this mapping effort is usually lost on the local infrastructure or in a GitHub repository, where it is hard to find and reuse. Even if data dependencies are not fed manually into the system, plenty of custom scripted solutions per application becoming quickly chaotic tend to grow, making applications harder to maintain and reproduce, finally leaving users and consumers with the resulting decreased reusability and unclear provenance.

While some of the conversion to popular formats is already performed by publishers, we argue that this should not be the burden of the data provider in general. Instead, we envision a software client, that - given a dependency configuration - can dump any data asset registered on a data management platform and converts it to a format supported by the target infrastructure. A client that can execute different applications and ingest data automatically, such that data is only one command away, like in traditional software dependency, built, and package management systems. Analogous to compiling of software, we define *compiling* of data as the process that converts, transforms or translates data geared to the needs of a specific target application.

The DBpedia Databus Client, that facilitates a more natural consumption and compiling of data from the DBpedia Databus and brings us one step closer towards our vision. Our main contributions are: a modular and extendable client that leads in combination with the Databus platform to less format conversion publishing effort (w.r.t. storage and time), enables easier and systematic data consumption with less conversion issues, allows for realizing data-driven apps using automatically updating data dependencies with clear provenance, and improves findability and reuse of mapping definitions.

## Example application deployment

1. Download the files of 5 datasets as given in the SPARQL query
2. Transform the compression of all files to `.bz2`
2. File format conversion
    1. convert all `RDF` files to `RDF-NTriple` files, and
    2. map the `.tsv` file from the second dataset to `RDF-NTriple` with this <databus-uri> `RML-Mapping`, and
    3. use this <databus-uri> `XSLT-Mapping` for the `.xml` file in the fifth dataset.
3. Load and deploy the processed data to a Virtuoso SPARQL Endpoint via Docker.