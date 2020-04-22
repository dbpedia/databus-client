package org.dbpedia.databus.client.api

import scala.collection.mutable

/**
 * Databus-Client API
 */
object DatabusClient {


  type QueryString = String
  type PathString = String

  def main(args: Array[String]): Unit = {

    //Example of how to use
//    DatabusClient.source("./src/query/query1").source("./src/query/query2").execute()

    DatabusClient.source("./src/resources/queries/example.sparql").compression(Compression.gz).format(Format.nt).execute()
  }

  /**
   * Sets the source the client needs to handle. A Source may be
   * a query string (SPARQL1.1) ,or
   * a string that contains a DBpedia Collection URI
   * a file, that either has the file extension .sparql or .query and contains a SPARQL query(queryFile), or contains data in a client-supported format, or
   * a dictionary that contains files in client-supported formats and/or queryFiles.
   *
   * @param source file or string
   * @return Inquirer
   */
  def source(source: String): Inquirer = {

    val inq = new Inquirer()
    inq.source(source)
  }

  class Inquirer() {

    private[this] val sources: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    def this(options: scala.collection.mutable.HashMap[String, String]) {
      this()
      this.options ++= options
    }

    /**
     * sets a source the Inquirer needs to handle
     * @param source file or string
     * @return Inquirer
     */
    def source(source: String): Inquirer = synchronized {
      sources += source
      this
    }

    /**
     * sets the desired format of the processed files
     *
     * @param format desired format
     * @return Inquirer
     */
    def format(format: Format.Value): Inquirer = config("format", format.toString)

    /**
     * sets the desired compression format of the processed files
     *
     * @param compression desired compression format
     * @return Inquirer
     */
    def compression(compression: Compression.Value): Inquirer = config("compression", compression.toString)

    /**
     * sets the target directory of the processed files
     *
     * @param targetDir target directory
     * @return Inquirer
     */
    def target(targetDir: String): Inquirer = config("target", targetDir)

    /**
     * adds a key value pair to the config
     *
     * @param key key
     * @param value value
     * @return Inquirer
     */
    def config(key: String, value: String): Inquirer = synchronized {
      options += key -> value
      this
    }

    /**
     * executes the Databus-Client with the prepared configuration
     */
    def execute(): Unit = {

      var array = Array[String]()
      if (options.contains("format")) array = array :+ "-f" :+ options.get("format").last
      if (options.contains("compression")) array = array :+ "-c" :+ options.get("compression").last
      if (options.contains("target")) array = array :+ "-d" :+ options.get("target").last
      if (options.contains("overwrite")) if (options.get("overwrite").last == "true") array = array :+ "-o"
      if (options.contains("clear")) if (options.get("clear").last == "true") array = array :+ "-c"

      sources.foreach(source => {
        //        println(s"source: $source")
        org.dbpedia.databus.client.main.Main.main(Array("-s", source) ++ array)
      })
    }

  }

  /**
   * supported formats
   */
  object Format extends Enumeration {
    type Format = Value
    val ttl, nt, tsv, rdfxml, jsonld = Value
  }

  /**
   * supported compression formats
   */
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
