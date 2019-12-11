package org.dbpedia.databus.filehandling

import better.files.File
import org.apache.http.HttpHeaders
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{BasicResponseHandler, HttpClientBuilder}
import org.dbpedia.databus.filehandling.download.Downloader
import org.slf4j.LoggerFactory


object SourceHandler {

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

  def handleQuery(query: String, target: File, cache: File, format: String, compression: String, overwrite: Boolean):Unit = {

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
      sha => FileHandler.handleFile(FileUtil.getFileWithSHA256(sha, cache), target, format, compression)
    )

  }

  def isSupportedOutFormat(format: String): Boolean = {
    if (format.matches("rdfxml|ttl|nt|jsonld|tsv|csv|same")) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Output file format $format is not supported.")
      println(s"Output file format $format is not supported.")
      false
    }
  }

  def isSupportedOutCompression(compression: String): Boolean = {
    if (compression.matches("bz2|gz|deflate|lzma|sz|xz|zstd||same")) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Output compression format $compression is not supported.")
      println(s"Output compression format $compression is not supported.")
      false
    }
  }

  def isCollection(str: String): Boolean = {
    val collection = """http[s]:\/\/.*\/.*\/collections\/.*""".r
    str match {
      case collection(_*) => true
      case _ => false
    }
  }

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
         |convert file(s) from $sourceType:\n${source}\n\nto destination:\n${target}

         |========================================================
      """.stripMargin

    println(str)
  }

}
