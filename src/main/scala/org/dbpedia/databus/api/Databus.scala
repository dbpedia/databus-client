package org.dbpedia.databus.api

import scala.collection.mutable


object Databus {


  type QueryString = String
  type PathString = String

  def main(args: Array[String]): Unit = {

    Databus.query("./src/query/query1").query("./src/query/query2").execute()

  }

  def query(query: QueryString): Inquirer = {

    val inq = new Inquirer()
    inq.query(query)
  }

  def source(source: String): Inquirer = {

    val inq = new Inquirer()
    inq.source(source)
  }

  class Inquirer() {

    private[this] val queries: mutable.ArrayBuffer[QueryString] = mutable.ArrayBuffer.empty[QueryString]
    private[this] val sources: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    def this(options: scala.collection.mutable.HashMap[String, String]) {
      this()
      this.options ++= options
    }

    def query(query: QueryString): Inquirer = synchronized {
      queries += query
      this
    }

    def source(source: String): Inquirer = synchronized {
      sources += source
      this
    }

    def format(format: Format.Value): Inquirer = config("format", format.toString)

    def compression(compression: Compression.Value): Inquirer = config("compression", compression.toString)

    def target(targetDir: String): Inquirer = config("target", targetDir)

    def config(key: String, value: String): Inquirer = synchronized {
      options += key -> value
      this
    }

    def execute(): Unit = {

      var array = Array[String]()
      if (options.contains("format")) array = array :+ "-f" :+ options.get("format").last
      if (options.contains("compression")) array = array :+ "-c" :+ options.get("compression").last
      if (options.contains("target")) array = array :+ "-d" :+ options.get("target").last
      if (options.contains("overwrite")) if (options.get("overwrite").last == "true") array = array :+ "-o"

      queries.foreach(query => {
        //        println(s"query $query")
        org.dbpedia.databus.main.Main.main(Array("-q", query) ++ array)
      })

      sources.foreach(source => {
        //        println(s"source: $source")
        org.dbpedia.databus.main.Main.main(Array("-s", source) ++ array)
      })
    }

  }

  object Format extends Enumeration {
    type Format = Value
    val ttl, nt, tsv, rdfxml, jsonld = Value
  }

  object Compression extends Enumeration {
    type Compression = Value
    val gz, bz2, deflate, lzma, sz, xz, zstd, None = Value
  }

  //  class Collection {
  //
  //    val query
  //
  //    val sfs = new mutable.ListBuffer[SingleFile]()
  //  }
  //
  //  class SingleFile {
  //
  //    def this(downloadURL: String) {
  //
  //    }
  //    downloadURL
  //    localPath
  //    grou
  //
  //  }
}
