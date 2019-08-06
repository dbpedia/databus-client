package org.dbpedia.databus

import better.files.File

object Main_Converter {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)

    println("Welcome to DBPedia - Convertertool")
    println("\n--------------------------------------------------------\n")

    println(s"""Convert all Files from\n"${conf.src_dir()}"\t to\n"${conf.targetrepo()}"""")
    println("\n--------------------------------------------------------\n")
    //  if "no" compression wanted change the value to an empty string
    var outputCompression = conf.outputCompression()
    if (conf.outputCompression()=="no") {
      outputCompression=""
    }

    println("Conversion:\n")
    val dir = File(conf.src_dir())
    val files = dir.listRecursively.toSeq
    for (file <- files) {
        if (! file.isDirectory){
          if (!file.name.equals("dataid.ttl")){
            println(s"InputFile: ${file.pathAsString}")
            FileHandler.convertFile(file, conf.src_dir(), conf.targetrepo(), conf.outputFormat(), outputCompression )
          }
        }
    }
//    var file = File("/home/eisenbahnplatte/git/dbpediaclient/downloaded_files/test/test2.ttl")
//    FileHandler.convertFile(file, conf.localrepo(), conf.outputFormat(), outputCompression )
  }

}
