package org.dbpedia.databus.client.filehandling.convert.format.tsd.lang

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSV extends TSDLang[DataFrame] {

  override def read(source: String, delimiter: Character = ',')(implicit sc: SparkContext): DataFrame = {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    spark.read.format("csv")
      .option("sep", delimiter.toString)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(source)
  }

  override def write(data: DataFrame, delimiter: Character = ',')(implicit sc: SparkContext): File = {
    data.coalesce(1).write
      .option("delimiter", delimiter.toString)
      .option("emptyValue", "")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)

    tempDir
  }

}
