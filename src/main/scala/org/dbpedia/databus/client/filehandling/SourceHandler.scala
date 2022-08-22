package org.dbpedia.databus.client.filehandling

import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.BeanProperty
import org.apache.http.HttpHeaders
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{BasicResponseHandler, HttpClientBuilder}
import org.dbpedia.databus.client.Config
import org.dbpedia.databus.client.filehandling.download.Downloader
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler
import org.dbpedia.databus.client.sparql.queries.DatabusQueries
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor


class SourceHandler(conf:CLI_Config) {

  //supported formats
  val fileFormats:String = Config.fileFormats
  val compressionFormats:String = Config.compressionFormats

  val cache: File = Config.cache
  val fileHandler = new FileHandler(conf)



  def execute(): Unit={
    if (File(conf.source()).exists()) {
      val sourceFile: File = File(conf.source())

      if (sourceFile.hasExtension && sourceFile.extension.get.matches(".sparql|.query")) { // conf.source() is a query file
        val queryString = FileUtil.readQueryFile(sourceFile)
        handleQuery(queryString, conf.endpoint())
      }
      else { // conf.source() is an already existing file or directory
        handleSource(sourceFile)
      }
    }
    else { // conf.source() is a query string or a collection uri

      if (isCollection(conf.source())) { // conf.source() is a collection uri
        val queryStr = getQueryOfCollection(conf.source())
        val endpoint = { // try to guess sparql endpoint if it is not set in program variables
          if (conf.endpoint.isDefined) conf.endpoint()
          else conf.source().split("/").take(3).mkString("/").concat("/sparql")
        }

        handleQuery(queryStr, endpoint)
      } else { // conf.source() is a query string
        handleQuery(conf.source(), conf.endpoint())
      }
    }



  }

  /**
   * convert input files in client-supported formats(no query files!) to the desired format and compression
   *
   * @param source file or directory
   */
  def handleSource(source: File):Unit = {
    printTask("source", source.pathAsString, File(conf.target()).pathAsString)
    println(s"CONVERSION TOOL:\n")

    val dataId_string = "dataid.jsonld"


    if (source.isDirectory) {
      val files = source.listRecursively.toSeq
      for (file <- files) {
        if (!file.isDirectory) {
          if (!file.name.equals(dataId_string)) {
            fileHandler.handleFile(file)
          }
        }
      }
    }
    else {
      fileHandler.handleFile(source)
    }
  }

  /**
   * download files of the input query and convert them to the desired format and compression
   *
   * @param query sparql query
   */
  def handleQuery(queryStr: String, endpoint:String):Unit = {

    printTask("query", queryStr, File(conf.target()).pathAsString)

    println("DOWNLOAD TOOL:")

    val allSHAs = Downloader.downloadWithQuery(queryStr, endpoint, cache, conf.overwrite())

    println("\n========================================================\n")
    println(s"CONVERSION TOOL:\n")

    allSHAs.foreach(
      sha => fileHandler.handleFile(FileUtil.getFileInCacheWithSHA256(sha, File("./target/databus.tmp/cache_dir/shas.txt")))
    )

  }

  def initialChecks():Unit={
    // check output format and compression
    if (!isSupportedOutFormat(conf.format())) System.exit(1)
    if (!isSupportedOutCompression(conf.compression())) System.exit(1)

    if (!conf.source.isDefined) {
      LoggerFactory.getLogger("Source Logger").error(s"No source found.")
      println(s"No source set.")
      System.exit(1)
    }

    if (conf.clear()) FileUtils.deleteDirectory(cache.toJava)
    cache.createDirectoryIfNotExists()

    val target = File(conf.target())
    target.createDirectoryIfNotExists()

    val temp = File("./target/databus.tmp/temp/")
    temp.createDirectoryIfNotExists()
  }

  /**
   * checks if desired format is supported
   * @param format input format
   * @return
   */
  def isSupportedOutFormat(format: String): Boolean = {
    if (format.matches(fileFormats)) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Output file format $format is not supported.")
      println(s"Output file format $format is not supported.")
      false
    }
  }

  /**
   * checks if desired compression is a supported
   * @param compression input compression
   * @return
   */
  def isSupportedOutCompression(compression: String): Boolean = {
    if (compression.matches(compressionFormats)) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Output compression format $compression is not supported.")
      println(s"Output compression format $compression is not supported.")
      false
    }
  }

  /**
   * checks if a string is a DBpedia collection
   * @param str string to check
   * @return
   */
  def isCollection(str: String): Boolean = {
    val collection = """http[s]?://.*/collections/.*""".r
    str match {
      case collection(_*) => true
      case _ => false
    }
  }

  /**
   * gets collection-related sparql query
   * @param uri collectionURI
   * @return query string
   */
  def getQueryOfCollection(uri: String): String = {
    val client = HttpClientBuilder.create().build()

    val httpGet = new HttpGet(uri)
    httpGet.addHeader(HttpHeaders.ACCEPT, "text/sparql")

    val response = client.execute(httpGet)
    val handler: ResponseHandler[String] = new BasicResponseHandler()

    handler.handleResponse(response)
  }

  def printTask(sourceType: String, source: String, target: String):Unit = {
    val str =
      s"""
         |========================================================
         |
         |TASK:
         |
         |convert file(s) from $sourceType:\n$source\n\nto destination:\n$target

         |========================================================
      """.stripMargin

    println(str)
  }
}
