package org.dbpedia.databus.client.filehandling

import better.files.File

object Config {

  //supported formats
  val fileFormats:String = "nt|ttl|rdfxml|owl|omn|owx|jsonld|tsv|csv|nq|trix|trig|same"
  val compressionFormats:String = "bz2|gz|deflate|lzma|sz|xz|zstd||same"

  val cache: File = File("./target/databus.tmp/cache_dir/")
}
