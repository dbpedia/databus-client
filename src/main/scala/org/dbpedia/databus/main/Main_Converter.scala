package org.dbpedia.databus.main

import better.files.File
import org.dbpedia.databus.FileHandler
import org.dbpedia.databus.cli.CLIConf

object Main_Converter {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)
    val dataId_string = "dataid.ttl"

    println("Welcome to DBPedia - Convertertool")
    println("\n--------------------------------------------------------\n")

    println(s"""convert all files from\n\nPATH: ${conf.source_dir()}\n\nto\n\nPATH: ${conf.destination_dir()}""")
    println("\n--------------------------------------------------------\n")

    //  if no compression wanted (output_compression not set) change the value to an empty string
    val outputCompression = conf.output_compression.isEmpty match {
      case true => ""
      case false => conf.output_compression()
    }

    println("Conversion:\n")
    val dir = File(conf.source_dir())
    val files = dir.listRecursively.toSeq
    for (file <- files) {
        if (! file.isDirectory){
          if (!file.name.equals(dataId_string)){
            println(s"input file:\t\t${file.pathAsString}")
            FileHandler.convertFile(file, conf.source_dir(), conf.destination_dir(), conf.output_format(), outputCompression )
          }
        }
    }
  }

}
