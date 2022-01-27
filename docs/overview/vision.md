# Vision
Any data on the bus can be made interoperable with application requirements. 
If an application can only read `RDF-NTriples` and `.gz`, but the desired data is only offered in `RDF-XML` and `.bz2`,
the client provides a `Download-As` functionality and transforms the data client-side. 
Files published on the Databus do not need to be offered in several formats.


### Example Application Deployment:

1. Download the files of 5 datasets as given in the SPARQL query
2. Transform all files to `.bz2`
2. Conversion
    1. convert all `RDF` files to `RDF-NTriple` files, and
    2. map the `.tsv` file from the second dataset to `RDF-NTriple` with this <databus-uri> `RML-Mapping`, and
    3. use this <databus-uri> `XSLT-Mapping` for the `.xml` file in the fifth dataset.
3. Finally, load and deploy a Virtuoso SPARQL Endpoint via Docker.