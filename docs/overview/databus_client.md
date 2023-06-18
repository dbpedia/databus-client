---
description: >-
  The DBpedia Databus Client simplifies data consumption and compilation from
  the DBpedia Databus, addressing challenges in using data from different
  publishers and domains.
---

# Databus Client

Data is often released in various serialization and compression formats, requiring conversion before it can be utilized. Additionally, data stored in relational databases or community-specific formats necessitates mapping for integration with knowledge graphs. Currently, mapping efforts are dispersed, leading to reduced reusability and unclear provenance.&#x20;

To address these issues, we propose a software client that can automatically convert and compile data assets registered on any DBpedia Databus distribution into formats supported by the target infrastructure. This client enables seamless consumption of compiled data, similar to traditional software dependency management systems. By shifting the burden of format conversion from data providers to the client, we reduce the publishing effort, enhance data consumption with fewer conversion problems, enable data-driven applications with automatically updated dependencies and enhances the findability and reuse of mapping definitions.

DBpedia Databus Client is a modular and extendable solution that brings us closer to realizing a unified and efficient data ecosystem, promoting reusability and maintaining clear provenance.



### Example Application Deployment

A user wants to load heterogeneous data from multiple DBpedia Databus distributions into a local application (Virtuoso SPARQL Endpoint).

1. Download the files of 5 datasets as given in the SPARQL query
2. Compression conversion: transform all files to `.bz2`
3. File format conversion:
   1. convert all `RDF` files to `RDF-NTriple` files, and
   2. map the `.tsv` file from the 3rd dataset to `RDF-NTriple` using this `RML-Mapping`
   3. use this `XSLT-Mapping` for the `.xml` file in the 5th dataset.
4. Load and deploy the processed data via Docker to a Virtuoso SPARQL Endpoint.
