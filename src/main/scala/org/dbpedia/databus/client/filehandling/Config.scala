package org.dbpedia.databus.client.filehandling

import better.files.File

object Config {

  //supported formats
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

  val fileFormats:String = (TSD ++ RDF_TRIPLES ++ RDF_QUADS :+ "same").mkString("|")
  val compressionFormats:String = "bz2|gz|deflate|lzma|sz|xz|zstd||same"

  val cache: File = File("./target/databus.tmp/cache_dir/")
}
