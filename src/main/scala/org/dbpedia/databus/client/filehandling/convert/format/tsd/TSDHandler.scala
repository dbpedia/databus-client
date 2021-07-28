package org.dbpedia.databus.client.filehandling.convert.format.tsd

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.tsd.lang.{CSV, TSV}
import org.dbpedia.databus.client.filehandling.convert.format.{EquivalenceClassHandler, tsd}
import org.dbpedia.databus.client.filehandling.convert.mapping.MappingInfo

/**
 * object to handle csv and tsv files
 */
object TSDHandler extends EquivalenceClassHandler[DataFrame]{

  /**
   * read file with tsv or csv format as Spark DataFrame
   *
   * @param source input file path
   * @param inputFormat input format
   * @return data in a Spark DataFrame
   */
  override def read(source: String, inputFormat: String, delimiter:Character)(implicit sparkContext: SparkContext): DataFrame = {

    inputFormat match {
      case "tsv" => TSV.read(source)
      case "csv" => CSV.read(source, delimiter)
    }
  }


  override def write(data:DataFrame, outputFormat: String, delimiter:Character)(implicit sparkContext: SparkContext): File = {

    outputFormat match {
      case "tsv" => TSV.write(data)
      case "csv" => CSV.write(data, delimiter)
    }
  }

}
