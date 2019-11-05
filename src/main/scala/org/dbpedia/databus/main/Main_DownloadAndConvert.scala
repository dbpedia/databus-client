package org.dbpedia.databus.main

import better.files.File
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.filehandling.converter.Converter
import org.dbpedia.databus.filehandling.downloader.Downloader
import org.dbpedia.databus.main.cli.CLIConf
import org.slf4j.LoggerFactory

object Main_DownloadAndConvert {

  def main(args: Array[String]) {

    println("Welcome to DBPedia - Download and Convert Tool")

    val conf = new CLIConf(args)
    val cache_dir = File("./cache_dir/")
    val dataId_string = "dataid.ttl"

    //    //  if no compression wanted (output_compression not set) change the value to an empty string
    //    val outputCompression = conf.output_compression.isEmpty match {
    //      case true => ""
    //      case false => conf.output_compression()
    //    }

    //Test if query is a File or a Query
    var queryString: String = ""
    if (File(conf.query()).exists()) {
      val file = File(conf.query())
      queryString = Downloader.readQueryFile(file)
    }
    else {
        queryString = conf.query()
    }

    println("\n========================================================\n")
    println("DOWNLOAD TOOL:")

    val allSHAs = Downloader.downloadWithQuery(queryString, cache_dir)

    println("\n========================================================\n")
    println("CONVERSION TOOL:\n")

    conf.format() match {
      case "rdfxml" | "ttl" | "nt" | "jsonld" | "tsv" | "same" =>
      case _ => {
        LoggerFactory.getLogger("File Format Logger").error(s"Input file format ${conf.format()} not supported.")
        println(s"Output file format ${conf.format()} not supported.")
      }
    }

    allSHAs.foreach(
      sha => Converter.convertFile(FileUtil.getFileWithSHA256(sha, cache_dir), cache_dir, File(conf.destination()), conf.format(), conf.compression())
    )

    //    val files = cache_dir.listRecursively.toSeq
    //    for (file <- files) {
    //      if (!file.isDirectory) {
    //        if (!file.name.equals(dataId_string)) {
    //          println(s"input file:\t\t${file.pathAsString}")
    //
    //        }
    //      }
    //      else if (file.name == "temp") { //Delete temp dir of previous failed run
    //        file.delete()
    //      }
    //    }

    //    download_temp.delete()
  }
}
