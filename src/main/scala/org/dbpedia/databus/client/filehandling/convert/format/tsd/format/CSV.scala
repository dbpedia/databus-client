package org.dbpedia.databus.client.filehandling.convert.format.tsd.format

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format

class CSV(delimiter: Character = ',') extends Format[DataFrame] {

  override def read(source: String): DataFrame = {
    Spark.session.read.format("csv")
      .option("sep", delimiter.toString)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(source)
  }

  override def write(data: DataFrame): File = {
    data.coalesce(1).write
      .option("delimiter", delimiter.toString)
      .option("emptyValue", "")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)

    FileUtil.unionFiles(tempDir, tempDir / "converted.csv")
  }

}
