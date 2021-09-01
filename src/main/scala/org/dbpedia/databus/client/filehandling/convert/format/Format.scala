package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File

trait Format[T] {

  val tempDir: File = File("./target/databus.tmp/temp_partialResults/")
  tempDir.delete(swallowIOExceptions = true)

  def read(source: String): T

  def write(data: T): File

}
