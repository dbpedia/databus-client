package org.dbpedia.databus.main

import better.files.File
import org.dbpedia.databus.FileHandler
import org.dbpedia.databus.cli.CLIConf
import org.dbpedia.databus.sparql.QueryHandler

object Main_DownloadAndConvert {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)
    val tempdir_download = "./tempdir_downloaded_files/"

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
    QueryHandler.executeDownloadQuery(queryString, tempdir_download)

    println("\n--------------------------------------------------------\n")

    //  if "no" compression wanted change the value to an empty string
    var outputCompression = conf.outputCompression()
    if (conf.outputCompression()=="no") {
      outputCompression=""
    }

    println("Conversion:\n")
    val dir = File(tempdir_download)
    val files = dir.listRecursively.toSeq
    for (file <- files) {
        if (! file.isDirectory){
          if (!file.name.equals("dataid.ttl")){
            println(s"InputFile: ${file.pathAsString}")
            FileHandler.convertFile(file, tempdir_download, conf.targetrepo(), conf.outputFormat(), outputCompression )
          }
        }
    }
//    var file = File("/home/eisenbahnplatte/git/dbpediaclient/downloaded_files/test/test2.ttl")
//    FileHandler.convertFile(file, conf.localrepo(), conf.outputFormat(), outputCompression )
  }

}
