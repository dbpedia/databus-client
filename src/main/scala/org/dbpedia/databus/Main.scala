package org.dbpedia.databus

import better.files.File

object Main {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)
    val dir_download = "./downloaded_files/"


    //Test if query is a File or a Query
    var queryString:String = ""
    File(conf.query()).exists() match {
      case true => {
        // "./src/query/query"
        val file = File(conf.query())
        queryString = FileHandler.readQuery(file)
      }
      case false => {
        queryString = conf.query()
      }
    }
    println(queryString)
    SelectQuery.execute(queryString)


    var dir = File(dir_download)
    var files = dir.listRecursively.toSeq
    for (file <- files) {
        if (! file.isDirectory){
          if (!file.name.equals("dataid.ttl")){
            FileHandler.convertFile(file, conf.localrepo(), conf.outputFormat(), conf.outputCompression() )
          }
        }
    }

  }

}
