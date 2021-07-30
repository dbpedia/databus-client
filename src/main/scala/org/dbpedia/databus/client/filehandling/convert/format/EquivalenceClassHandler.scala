package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File
import org.apache.spark.SparkContext

trait EquivalenceClassHandler[T] {

  val tempDir: File = File("./target/databus.tmp/temp/")

  def read(source: String, inputFormat: String): T

  def write(data: T, outputFormat: String): File

}
