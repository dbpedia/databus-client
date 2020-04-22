package org.dbpedia.databus.client.filehandling

import better.files.File
import org.apache.http.HttpHeaders
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{BasicResponseHandler, HttpClientBuilder}
import org.dbpedia.databus.client.filehandling.download.Downloader
import org.slf4j.LoggerFactory


object SourceHandler {

  /**
   * convert input files in client-supported formats(no query files!) to the desired format and compression
   *
   * @param source file or directory
   * @param target target directory
   * @param format desired format
   * @param compression desired compression
   */
  def handleSource(source: File, target: File, format: String, compression: String):Unit = {
    printTask("source", source.pathAsString, target.pathAsString)

    println(s"CONVERSION TOOL:\n")

    val dataId_string = "dataid.ttl"

    if (source.isDirectory) {
      val files = source.listRecursively.toSeq
      for (file <- files) {
        if (!file.isDirectory) {
          if (!file.name.equals(dataId_string)) {
            FileHandler.handleFile(file, target, format, compression)
          }
        }
      }
    }
    else {
      FileHandler.handleFile(source, target, format, compression)
    }
  }

  /**
   * download files of the input query and convert them to the desired format and compression
   *
   * @param query sparql query
   * @param target target directory
   * @param cache cache directory
   * @param format output format
   * @param compression output compression
   * @param overwrite overwrite files in the cache directory
   */
  def handleQuery(query: String, target: File, cache: File, format: String, compression: String, overwrite: Boolean=false):Unit = {

    val queryStr = {
      if (isCollection(query)) getQueryOfCollection(query)
      else query
    }

    printTask("query", queryStr, target.pathAsString)

    println("DOWNLOAD TOOL:")

    val allSHAs = Downloader.downloadWithQuery(queryStr, cache, overwrite)

    println("\n========================================================\n")
    println(s"CONVERSION TOOL:\n")

    allSHAs.foreach(
      sha => FileHandler.handleFile(FileUtil.getFileInCacheWithSHA256(sha, File("./target/databus.tmp/cache_dir/shas.txt")), target, format, compression)
    )

  }

  /**
   * checks if desired format is supported
   * @param format input format
   * @return
   */
  def isSupportedOutFormat(format: String): Boolean = {
    if (format.matches("rdfxml|ttl|nt|jsonld|tsv|csv|same")) true
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
    if (compression.matches("bz2|gz|deflate|lzma|sz|xz|zstd||same")) true
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
    val collection = """http[s]?://databus.dbpedia.org/.*/collections/.*""".r
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
