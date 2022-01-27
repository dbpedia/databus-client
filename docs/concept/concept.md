# Concept

The Databus-Client is designed to unify and convert data on the client-side in several layers:

| Level | Client Action | Implemented formats
|---|---|---|
| 1 |  Download As-Is | All files on the [databus](https://databus.dbpedia.org)
| 2 |  Unify compression | bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate (, no compression)
| 3 |  Unify isomporphic formats | `Download as` implemented for {nt, ttl, rdfxml}, {nq, trix, trig, json-ld}, {tsv,csv}
| 4 |  Transform with mappings | {nt, ttl, rdfxml} <--> {csv, tsv} <--> {nq, trig, trix, json-ld}


### Roadmap for levels

* Level 1: all features finished, testing required
* Level 2: using Apache Compress library covers most of the compression formats, more testing required
* Level 3: Scalable RDF libraries from [SANSA-Stack](http://sansa-stack.net/) and [Databus Derive](https://github.com/dbpedia/databus-derive). Step by step, extension for all (quasi-)isomorphic [IANA mediatypes](https://www.iana.org/assignments/media-types/media-types.xhtml).
* Level 4: In addition, we plan to provide a plugin mechanism to incorporate more sophisticated format.mapping engines as [Tarql](https://tarql.github.io/) (already implemented), [RML](http://rml.io), R2RML, [R2R](http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/) (for owl:equivalence translation) and XSLT.
