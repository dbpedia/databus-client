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
    QueryHandler.executeDownloadQuery(queryString, conf.destination_dir())

    println("\n--------------------------------------------------------\n")
    println(s"Files have been downloaded to ${conf.destination_dir()}")
  }

}
