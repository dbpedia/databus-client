package org.dbpedia.databus.client.filehandling.convert.format.csv

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.csv
import org.dbpedia.databus.client.filehandling.convert.mapping.MappingInfo

/**
 * object to handle csv and tsv files
 */
object CSVHandler {

  /**
   * read file with tsv or csv format as Spark DataFrame
   *
   * @param inputFile input file
   * @param inputFormat input format
   * @param spark sparkSession
   * @return data in a Spark DataFrame
   */
  def read(inputFile: File, inputFormat: String, spark: SparkSession, delimiter:Character): DataFrame = {

    inputFormat match {
      case "tsv" =>
        Reader.csv_to_df(inputFile.pathAsString, '\t', spark)
      case "csv" =>
        Reader.csv_to_df(inputFile.pathAsString, delimiter, spark)
    }

  }


  def write(tempDir:File, data:DataFrame, outputFormat: String, spark: SparkSession, delimiter:Character): Unit = {

    outputFormat match {
      case "tsv" =>
        Writer.writeDF(data, tempDir, "\t", "true")
      case "csv" =>
        Writer.writeDF(data, tempDir, delimiter.toString, "true")
    }
  }


  def readAsTriples(inputFile: File, inputFormat: String, spark: SparkSession, mappingInfo: MappingInfo): RDD[Triple] = {

    inputFormat match {
      case "tsv" =>
        csv.Reader.csv_to_rddTriple(mappingInfo.mappingFile, inputFile.pathAsString, '\t', sc = spark.sparkContext)

      case "csv" =>
//        val delimiter = {
//          if (mappingInfo.length > 1) mappingInfo(1)
//          else scala.io.StdIn.readLine("Please type delimiter of CSV file:\n")
//        }
//
//        val quotation = {
//          if (mappingInfo.length > 1)  mappingInfo(2)
//          else scala.io.StdIn.readLine("Please type quote character of CSV file:\n(e.g. ' \" ' for double quoted entries or ' null ' if there's no quotation)\n")
//        }
//
//        val delimiterChar = delimiter.toCharArray.apply(0).asInstanceOf[Character]
//        val quoteChar = quotation match {
//          case "null" => null
//          case _ => quotation.toCharArray.apply(0).asInstanceOf[Character]
//        }

        csv.Reader.csv_to_rddTriple(mappingInfo.mappingFile, inputFile.pathAsString, mappingInfo.delimiter, mappingInfo.quotation, spark.sparkContext)
    }
  }


  def writeTriples(tempDir: File, data: RDD[Triple], outputFormat: String, delimiter:Character, spark: SparkSession, createMappingFile:Boolean=true): File = {
    outputFormat match {
      case "tsv" =>
        Writer.writeTriples(data, "\t", tempDir, spark, createMappingFile)
      case "csv" =>
        Writer.writeTriples(data, delimiter.toString, tempDir, spark, createMappingFile)
    }

  }

}
