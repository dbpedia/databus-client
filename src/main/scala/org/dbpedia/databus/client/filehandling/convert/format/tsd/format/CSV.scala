package org.dbpedia.databus.client.filehandling.convert.format.tsd.format

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClass

class CSV(delimiter: Character = ',') extends EquivalenceClass[DataFrame] {

  override def read(source: String)(implicit sc: SparkContext): DataFrame = {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    spark.read.format("csv")
      .option("sep", delimiter.toString)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(source)
  }

  override def write(data: DataFrame)(implicit sc: SparkContext): File = {
    data.coalesce(1).write
      .option("delimiter", delimiter.toString)
      .option("emptyValue", "")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)

    tempDir
  }

}
