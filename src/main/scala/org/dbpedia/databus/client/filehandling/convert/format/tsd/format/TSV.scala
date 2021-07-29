package org.dbpedia.databus.client.filehandling.convert.format.tsd.format

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClass

class TSV extends EquivalenceClass[DataFrame] {

  val delimiter: Character = '\t'

  override def read(source: String)(implicit sc: SparkContext): DataFrame = {
    new CSV(delimiter).read(source)
  }

  override def write(data: DataFrame)(implicit sc: SparkContext): File = {
    new CSV(delimiter).write(data)
  }

}
