package org.dbpedia.databus.filehandling

import better.files.File
import org.dbpedia.databus.filehandling.converter.Converter
import org.dbpedia.databus.filehandling.downloader.Downloader
import org.slf4j.LoggerFactory

object FileHandler {

  def handleSource(source:File, target:File, format:String, compression:String) ={
    printTask("source", source.pathAsString, target.pathAsString)

    println(s"CONVERSION TOOL:\n")

    val dataId_string = "dataid.ttl"

    if (source.isDirectory) {
      val files = source.listRecursively.toSeq
      for (file <- files) {
        if (!file.isDirectory) {
          if (!file.name.equals(dataId_string)) {
            Converter.convertFile(file, target, format, compression)
          }
        }
      }
    }
    else {
      Converter.convertFile(source, target, format, compression)
    }
  }

  def handleQuery(query:String, target:File, cache:File, format:String, compression:String, overwrite:Boolean) ={
    printTask("query", query, target.pathAsString)

    println("DOWNLOAD TOOL:")

    val allSHAs = Downloader.downloadWithQuery(query, cache, overwrite)

    println("\n========================================================\n")
    println(s"CONVERSION TOOL:\n")

    allSHAs.foreach(
      sha => Converter.convertFile(FileUtil.getFileWithSHA256(sha, cache), target, format, compression)
    )

  }

  def printTask(sourceType:String, source:String, target:String) ={
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
//    println("\n========================================================\n")
//    println()
//    println(s"""convert file(s) from $str:\n${source}\n\nto destination:\n${target}""")
//    println("\n========================================================\n")
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
    if (compression.matches("bz2|gz|deflate|lzma|sz|xz|zstd|''|same")) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Output compression format $compression is not supported.")
      println(s"Output file format $compression is not supported.")
      false
    }
  }
}
