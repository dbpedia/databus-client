# Roadmap

### Roadmap for [levels](concept.md)

* Level 1: all features finished, testing required
* Level 2: using Apache Compress library covers most of the compression formats, more testing required
* Level 3: Scalable RDF libraries from [SANSA-Stack](http://sansa-stack.net/) and [Databus Derive](https://github.com/dbpedia/databus-derive). Step by step, extension for all (quasi-)isomorphic [IANA mediatypes](https://www.iana.org/assignments/media-types/media-types.xhtml).
* Level 4: In addition, we plan to provide a plugin mechanism to incorporate more sophisticated format.mapping engines as [Tarql](https://tarql.github.io/) (already implemented), [RML](http://rml.io), R2RML, [R2R](http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/) (for owl:equivalence translation) and XSLT.

### Current State

**Beta**:
The Databus Client should produce expected results for compression and RDF format.conversion. Errors could occure for the mapping process. Please expect some code refactoring and fluctuation. There will be an open-source licence, either GPL or Apache.