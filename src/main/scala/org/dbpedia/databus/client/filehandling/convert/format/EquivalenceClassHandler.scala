package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

trait EquivalenceClassHandler[T] {

  val tempDir: File = File("./target/databus.tmp/temp/")

  def read(source: String, inputFormat: String, baseURL:String=""): T

  def write(data: T, outputFormat: String, baseURL:String=""): File

}
