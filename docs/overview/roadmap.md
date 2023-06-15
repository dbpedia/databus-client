# Features

<table><thead><tr><th width="155">Layer</th><th>Implemented formats</th><th>Future Work</th></tr></thead><tbody><tr><td>Download-Layer</td><td><ul><li>All files on the <a href="https://github.com/dbpedia/databus">DBpedia Databus</a></li></ul></td><td><ul><li>All features finished</li></ul></td></tr><tr><td>Compression-Layer</td><td><ul><li>bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate</li></ul></td><td><ul><li>Additional frameworks will be included upon demand.</li></ul></td></tr><tr><td>File-Format-Layer</td><td><ul><li>RDF-Triples -> {nt, ttl, rdfxml, hdt, owl, omn, owx}</li><li>RDF-Quads -> {nq, trix, trig, json-ld} </li><li>TSD -> {tsv, csv}</li></ul></td><td><ul><li>scalable RDF libraries from <a href="http://sansa-stack.net/">SANSA-Stack</a> and <a href="https://github.com/dbpedia/databus-derive">Databus Derive</a></li><li>step by step, extension for all (quasi-)isomorphic <a href="https://www.iana.org/assignments/media-types/media-types.xhtml">IANA mediatypes</a></li></ul></td></tr><tr><td>Mapping-Layer</td><td><ul><li>RDF-Triples &#x3C;--> RDF-Quads</li><li>RDF-Triples &#x3C;--> TSD </li><li>RDF-Quads --> TSD</li></ul></td><td><ul><li>Provide a plugin mechanism to incorporate more sophisticated format.mapping engines as <a href="http://rml.io">RML</a>, R2RML, <a href="http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/">R2R</a> (for owl:equivalence translation) and XSLT.</li></ul></td></tr></tbody></table>

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
