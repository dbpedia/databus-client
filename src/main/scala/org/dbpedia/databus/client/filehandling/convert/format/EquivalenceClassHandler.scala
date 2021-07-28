package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File
import org.apache.spark.SparkContext

trait EquivalenceClassHandler[T] {

  def read(source: String, inputFormat: String, delimiter:Character=',')(implicit sc: SparkContext): T

  def write(data: T, outputFormat: String, delimiter:Character=',')(implicit sc: SparkContext): File

}
