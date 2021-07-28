package org.dbpedia.databus.client.filehandling.convert.format.tsd.lang

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

trait TSDLang[T] {

  val tempDir: File = File("./target/databus.tmp/temp/")
  if (tempDir.exists) tempDir.delete()

  def read(source: String, delimiter: Character = ',')(implicit sc: SparkContext): T

  def write(data: DataFrame, delimiter: Character = ',')(implicit sc: SparkContext): File

}
