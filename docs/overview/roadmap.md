# Roadmap

## Current State

**Beta**:
The Databus Client should produce expected results for compression conversion and file format conversion. Errors could occure for the mapping process. Please expect some code refactoring and fluctuation.

### Limitations of mappings
**TSD -> RDF Quads**: Due to the limitations of Tarql, there is no mapping from TSD to RDF Quads possible at the moment.    
**RDF Triples -> TSD**: The resul

## Supported formats

| Level | Layer             | Implemented formats                                                                       |
| ----- | ----------------- |-------------------------------------------------------------------------------------------|
| 1     | Download-Layer    | All files on the [DBpedia Databus](https://github.com/dbpedia/databus)                    |
| 2     | Compression-Layer | bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate (, no compression)                    |
| 3     | File-Format-Layer | {nt, ttl, rdfxml, hdt, owl, omn, owx} , <br/> {nq, trix, trig, json-ld} , <br/> {tsv,csv} |
| 4     | Mapping-Layer     | RDF-Triples {nt, ttl, ...} <--> TSD {csv, tsv} <--> RDF-Quads {nq, trig, ...}             |

## Roadmap for [Layers](concept.md)

### Download-Layer
 * All features finished

### Compression-Layer
 * Using Apache Compress library covers most of the compression formats
 * Additional frameworks will be included upon demand

### File-Format-Layer

#### Implemented features
* [Apache Jena Framework](https://jena.apache.org/index.html) -> nt, ttl, rdfxml, nq, trix, trig, json-ld
* [RDF HDT Framework](https://www.rdfhdt.org/) -> hdt
* [OWL API](https://github.com/owlcs/owlapi) -> owl, omn, owx

#### Future Work
* Scalable RDF libraries from [SANSA-Stack](http://sansa-stack.net/) and [Databus Derive](https://github.com/dbpedia/databus-derive)
* Step by step, extension for all (quasi-)isomorphic [IANA mediatypes](https://www.iana.org/assignments/media-types/media-types.xhtml).

### Mapping-Layer

#### Implemented features
 * Tarql has been implemented for mapping from TSD to RDF
 * 

#### Future Work
 * We plan to provide a plugin mechanism to incorporate more sophisticated format.mapping engines as [RML](http://rml.io), R2RML, [R2R](http://wifo5-03.informatik.uni-mannheim.de/bizer/r2r/) (for owl:equivalence translation) and XSLT.
 