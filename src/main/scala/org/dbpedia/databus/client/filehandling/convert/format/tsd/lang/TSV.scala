package org.dbpedia.databus.client.filehandling.convert.format.tsd.lang

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

object TSV extends TSDLang[DataFrame] {

  override def read(source: String, delimiter: Character = '\t')(implicit sc: SparkContext): DataFrame = {
    CSV.read(source, delimiter)
  }

  override def write(data: DataFrame, delimiter: Character = '\t')(implicit sc: SparkContext): File = {
    CSV.write(data, delimiter)
  }

}
