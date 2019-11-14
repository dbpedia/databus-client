package org.dbpedia.databus.main

import better.files.File
import org.dbpedia.databus.filehandling.converter.Converter
import org.dbpedia.databus.main.cli.CLIConf
import org.slf4j.LoggerFactory

object Main_Converter {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)
    val dataId_string = "dataid.ttl"

    println("Welcome to DBPedia - Conversion tool")
    println("\n--------------------------------------------------------\n")

    println(s"""convert file(s) from:\n\n${conf.source()}\n\nto:\n\n${conf.destination()}""")
    println("\n--------------------------------------------------------\n")

    conf.format() match {
      case "rdfxml" | "ttl" | "nt" | "jsonld" | "tsv" | "same" =>

      case _ =>
        LoggerFactory.getLogger("File Format Logger").error(s"Input file format ${conf.format()} not supported.")
        println(s"Output file format ${conf.format()} not supported.")
    }

    val source = File(conf.source())
    val destination_dir = File(conf.destination())

    if (source.isDirectory) {
      val files = source.listRecursively.toSeq
      for (file <- files) {
        if (!file.isDirectory) {
          if (!file.name.equals(dataId_string)) {
            Converter.convertFile(file, destination_dir, conf.format(), conf.compression())
          }
        }
        else if (file.name == "temp") { //Delete temp dir of previous failed run
          file.delete()
        }
      }
    }
    else {
      Converter.convertFile(source, destination_dir, conf.format(), conf.compression())
    }

  }

}
