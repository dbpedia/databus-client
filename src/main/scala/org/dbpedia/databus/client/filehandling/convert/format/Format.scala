package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File
import org.apache.spark.SparkContext

trait Format[T] {

  val tempDir: File = File("./target/databus.tmp/temp/")
  if (tempDir.exists) tempDir.delete()

  def read(source: String): T

  def write(data: T): File

}
