# Features

## Concept

The client is a modular system designed for high reusability, with components like the downloading and compression converter available interchangeably. It operates across four functionality layers.

<figure><img src="../img/concept.png" alt=""><figcaption><p>data flow of DBpedia's Databus Client</p></figcaption></figure>

**Download-Layer:** This layer downloads data assets from the DBpedia Databus, preserving their provenance through stable file identifiers and additional metadata. It allows fine-grained selection of data assets through an interoperable data dependency specification and compiling configurations.

**Compression-Layer:** If conversion is needed, this layer detects the input compression format, decompresses the file, and passes it to the Format-Layer if necessary. It then compresses the converted file into the desired output compression format and returns it to the Download-Layer.

**File-Format-Layer:** This layer handles data format conversion, utilizing the Format-Layer and Mapping-Layer as required. It parses the uncompressed file into a unified internal data structure for the corresponding format equivalence class. The Format-Layer serializes this data structure into the desired output format and sends it back to the Compression-Layer.

**Mapping-Layer:** Used when the input and output formats belong to different equivalence classes or require data manipulation. Mapping configurations are used to transform the data from the input equivalence class to the internal data structure of the target format. The transformed data is then passed back to the Format-Layer.

Databus Client's modular design enables efficient data processing, conversion, and manipulation, enhancing reusability and flexibility in data management.

## Features

<table><thead><tr><th width="155">Layer</th><th>Implemented formats</th><th>Future Work</th></tr></thead><tbody><tr><td>Download-Layer</td><td><ul><li>All files on the <a href="https://github.com/dbpedia/databus">DBpedia Databus</a></li></ul></td><td><ul><li>All features finished</li></ul></td></tr><tr><td>Compression-Layer</td><td><ul><li>bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate</li></ul></td><td><ul><li>Additional formats will be included upon demand.</li></ul></td></tr><tr><td>File-Format-Layer</td><td><ul><li>RDF-Triples: {nt, ttl, rdfxml, hdt, owl, omn, owx}</li><li>RDF-Quads: {nq, trix, trig, json-ld} </li><li>TSD: {tsv, csv}</li></ul></td><td><ul><li>scalable RDF libraries from <a href="http://sansa-stack.net/">SANSA-Stack</a> and <a href="https://github.com/dbpedia/databus-derive">Databus Derive</a></li><li>step by step, extension for all (quasi-)isomorphic <a href="https://www.iana.org/assignments/media-types/media-types.xhtml">IANA mediatypes</a></li></ul></td></tr><tr><td>Mapping-Layer</td><td><ul><li>RDF-Triples &#x3C;--> RDF-Quads</li><li>RDF-Triples &#x3C;--> TSD </li><li>RDF-Quads --> TSD</li><li><del>TSD --> RDF-Quads</del></li></ul></td><td><ul><li>Provide a plugin mechanism to incorporate more sophisticated format.mapping engines as <a href="http://rml.io">RML</a>, R2RML, <a href="http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/">R2R</a> (for owl:equivalence translation) and XSLT.</li></ul></td></tr></tbody></table>

## Used frameworks

* **Compression-Layer:**&#x20;
  * [Apache Compress library](https://commons.apache.org/proper/commons-compress/) covers most of the compression formats.
* **File-Format-Layer:**
  * [Apache Jena Framework](https://jena.apache.org/index.html) -> nt, ttl, rdfxml, nq, trix, trig, json-ld
  * [RDF HDT Framework](https://www.rdfhdt.org/) -> hdt
  * [OWL API](https://github.com/owlcs/owlapi) -> owl, omn, owx
* **Mapping-Layer:**
  * [Tarql](https://tarql.github.io/) has been implemented for mapping from TSD to RDF Triples.
  * Apache Jena and Apache Spark are used to achieve the RDF Triples to TSD mapping.
  * Apache Jena is used for RDF Triples <-> RDF Quads mappings.

## Limitations of mappings

* **TSD -> RDF Quads**: Due to the limitations of Tarql, there is no mapping from TSD to RDF Quads possible at the moment.
* **RDF Triples -> TSD**: The mapping results in a wide table, no more precise mapping is possible yet.



###
