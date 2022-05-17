package org.dbpedia.databus.client

import better.files.File

object Config {

  val endpoint = "http://localhost:3000/sparql"
  val graphsURI = "http://localhost:3002/g/"

  val cache: File = File("./target/databus.tmp/cache_dir/")

  //supported file formats in its equivalence classes
  val RDF_TRIPLES: Seq[String] = Seq(
    "hdt",
    "ttl",
    "nt",
    "rdfxml",
    "owl",
    "omn"
  )

  val RDF_QUADS: Seq[String] = Seq(
    "nq",
    "trix",
    "trig",
    "jsonld"
  )

  val TSD: Seq[String] = Seq(
    "tsv",
    "csv"
  )

  //supported file formats
  val fileFormats: String = (TSD ++ RDF_TRIPLES ++ RDF_QUADS :+ "same").mkString("|")

  //supported compression formats
  val compressionFormats: String = "bz2|gz|deflate|lzma|sz|xz|zstd||same"


}
