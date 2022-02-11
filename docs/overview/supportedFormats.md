# Supported formats

| Level | Layer             | Implemented formats                                                                       |
| ----- | ----------------- |-------------------------------------------------------------------------------------------|
| 1     | Download-Layer    | All files on the [DBpedia Databus](https://github.com/dbpedia/databus)                    |
| 2     | Compression-Layer | bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate (, no compression)                    |
| 3     | File-Format-Layer | {nt, ttl, rdfxml, hdt, owl, omn, owx} , <br/> {nq, trix, trig, json-ld} , <br/> {tsv,csv} |
| 4     | Mapping-Layer     | RDF-Triples {nt, ttl, ...} <--> TSD {csv, tsv} <--> RDF-Quads {nq, trig, ...}             |

The Layers are described in detail in the [Concept](docs/overview/concept.md).