# Concept

The Databus-Client is designed to unify and convert data on the client-side in several layers:

| Level | Client Action | Implemented formats
|---|---|---|
| 1 |  Download As-Is | All files on the [databus](https://databus.dbpedia.org)
| 2 |  Unify compression | bz2, gz, br, lzma, xz, zstd, snappy-framed, deflate (, no compression)
| 3 |  Unify isomporphic formats | `Download as` implemented for {nt, ttl, rdfxml}, {nq, trix, trig, json-ld}, {tsv,csv}
| 4 |  Transform with mappings | {nt, ttl, rdfxml} <--> {csv, tsv} <--> {nq, trig, trix, json-ld}