package org.dbpedia.databus

import org.rogach.scallop._



class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""

Example: scala main.scala -q ./src/query/downloadquery  --repo converted_files/ --compression gz -f ttl

For usage see below:
    """)

  val query = opt[String]("query", default= Some("./src/query/query"), descr = "any ?file query; You can pass the query directly or save it in a textfile and pass the filepath")
  val localrepo = opt[String]("repo", default= Some("./converted_files/"), descr = "set the destination directory for converted files")
  val outputFormat = opt[String]("format", default= Some("ttl"), descr = "set the fileformat of the outputfile")
  val outputCompression = opt[String]("compression", default= Some("bz2"), descr = "set the compressionformat of the outputfile")
  val help = opt[Boolean]("help", noshort = true, descr = "Show this message")

  verify()

}



