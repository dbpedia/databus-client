package org.dbpedia.databus.main

import better.files.File
import org.dbpedia.databus.FileHandler
import org.dbpedia.databus.cli.CLIConf
import org.dbpedia.databus.sparql.QueryHandler

object Main_Downloader {

  def main(args: Array[String]) {

    println("Welcome to DBPedia - Downloadtool")
    println("\n--------------------------------------------------------\n")

    val conf = new CLIConf(args)
    val download_temp = File("./tempdir_downloaded_files/")
    val dataId_string = "dataid.ttl"

    //Test if query is a File or a Query
    var queryString:String = ""
    File(conf.query()).exists() match {
      case true => {
        // "./src/query/query"
        val file = File(conf.query())
        queryString = FileHandler.readQueryFile(file)
      }
      case false => {
        queryString = conf.query()
      }
    }

    println(s"DownloadQuery: \n\n$queryString")
    println("--------------------------------------------------------\n")
    println("Files to download:")
    QueryHandler.executeDownloadQuery(queryString, download_temp)

    val files = download_temp.listRecursively.toSeq
    for (file <- files) {
      if (! file.isDirectory){
        if (!file.name.equals(dataId_string)){
          FileHandler.copyUnchangedFile(file, download_temp, File(conf.destination_dir()))
        }
      }
      else if (file.name == "temp") { //Delete temp dir of previous failed run
        file.delete()
      }
    }

    println("\n--------------------------------------------------------\n")
    println(s"Files have been downloaded to ${conf.destination_dir()}")
    download_temp.delete()
  }

}
