package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File
import org.dbpedia.databus.client.sparql.QueryHandler.getClass
import org.slf4j.{Logger, LoggerFactory}

trait Format[T] {

  val tempDir: File = File("./target/databus.tmp/temp_partialResults/")
  tempDir.delete(swallowIOExceptions = true)
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def read(source: String): T

  def write(data: T): File

}
