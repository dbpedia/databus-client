package org.dbpedia.databus.client.main

import better.files.File
import org.apache.commons.io.FileUtils
import org.dbpedia.databus.client.filehandling.{FileUtil, SourceHandler}
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {

    println("DBpedia - Databus-Client")

    val conf = new CLI_Config(args)
    val sourceHandler = new SourceHandler(conf)

    sourceHandler.initialChecks()
    sourceHandler.execute()
  }


}
