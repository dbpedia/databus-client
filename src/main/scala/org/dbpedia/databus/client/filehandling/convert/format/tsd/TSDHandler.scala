package org.dbpedia.databus.client.filehandling.convert.format.tsd

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.tsd.format.{CSV, TSV}
import org.dbpedia.databus.client.filehandling.convert.format.{EquivalenceClassHandler, tsd}
import org.dbpedia.databus.client.filehandling.convert.mapping.util.MappingInfo

/**
 * object to handle csv and tsv files
 */
class TSDHandler(delimiter:Character=',') extends EquivalenceClassHandler[DataFrame]{

  /**
   * read file with tsv or csv format as Spark DataFrame
   *
   * @param source input file path
   * @param inputFormat input format
   * @return data in a Spark DataFrame
   */
  override def read(source: String, inputFormat: String, baseURI:String=""): DataFrame = {

    inputFormat match {
      case "tsv" => new TSV().read(source)
      case "csv" => new CSV(delimiter).read(source)
    }
  }


  override def write(data:DataFrame, outputFormat: String, baseURI:String=""): File = {

    outputFormat match {
      case "tsv" => new TSV().write(data)
      case "csv" => new CSV(delimiter).write(data)
    }
  }

}
