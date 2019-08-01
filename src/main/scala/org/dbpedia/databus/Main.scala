package org.dbpedia.databus

import better.files.File

object Main {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)
    val dir_download = "./downloaded_files/"

//    //Test if query is a File or a Query
//    var queryString:String = ""
//    File(conf.query()).exists() match {
//      case true => {
//        // "./src/query/query"
//        val file = File(conf.query())
//        queryString = FileHandler.readQueryFile(file)
//      }
//      case false => {
//        queryString = conf.query()
//      }
//    }
//    println(s"DownloadQuery: \n\n$queryString")
//    println("--------------------------------------------------------\n")
//    println("Files to download:")
//    QueryHandler.executeDownloadQuery(queryString)
//
//    println("\n--------------------------------------------------------\n")

    //  if "no" compression wanted change the value to an empty string
    var outputCompression = conf.outputCompression()
    if (conf.outputCompression()=="no") {
      outputCompression=""
    }

//    println("Conversion:\n")
//    val dir = File(dir_download)
//    val files = dir.listRecursively.toSeq
//    for (file <- files) {
//        if (! file.isDirectory){
//          if (!file.name.equals("dataid.ttl")){
//            println(s"InputFile: ${file.pathAsString}") //${conf.outputFormat()}.${outputCompression}
//            FileHandler.convertFile(file, conf.localrepo(), conf.outputFormat(), outputCompression )
//          }
//        }
//    }
    var file = File("/home/eisenbahnplatte/git/dbpediaclient/downloaded_files/test/test2.ttl")
    FileHandler.convertFile(file, conf.localrepo(), conf.outputFormat(), outputCompression )
  }

}
