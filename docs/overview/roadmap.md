# Roadmap

## Current State

**Beta**:
The Databus Client should produce expected results for compression and RDF format.conversion. Errors could occure for the mapping process. Please expect some code refactoring and fluctuation.

## Roadmap for [Layers](concept.md)

### Download-Layer
 * All features finished

### Compression-Layer
 * Using Apache Compress library covers most of the compression formats
 * Additional frameworks will be included upon demand

### File-Format-Layer
 * Scalable RDF libraries from [SANSA-Stack](http://sansa-stack.net/) and [Databus Derive](https://github.com/dbpedia/databus-derive)
 * Step by step, extension for all (quasi-)isomorphic [IANA mediatypes](https://www.iana.org/assignments/media-types/media-types.xhtml).

### Mapping-Layer
 * Tarql has been implemented for mapping from TSD to RDF
 * We plan to provide a plugin mechanism to incorporate more sophisticated format.mapping engines as [RML](http://rml.io), R2RML, [R2R](http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/) (for owl:equivalence translation) and XSLT.
 