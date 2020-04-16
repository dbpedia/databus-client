package org.dbpedia.databus.client.filehandling.convert.format.csv

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.csv

object CSVHandler {

  def read(inputFile: File, inputFormat: String, spark: SparkSession): DataFrame = {

    inputFormat match {
      case "tsv" =>
        Reader.csv_to_df(inputFile.pathAsString, '\t', spark)
      case "csv" =>
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).asInstanceOf[Character]
        Reader.csv_to_df(inputFile.pathAsString, delimiter, spark)
    }

  }

  def write(tempDir:File, data:DataFrame, outputFormat: String, spark: SparkSession): Unit = {

    outputFormat match {
      case "tsv" =>
        Writer.writeDF(data, tempDir, "\t", "true")
      case "csv" =>
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n")
        Writer.writeDF(data, tempDir, delimiter, "true")
    }
  }


  def readAsTriples(inputFile: File, inputFormat: String, spark: SparkSession, mappingInformation: Seq[String]): RDD[Triple] = {

    val mappingFile = {
      if(mappingInformation.isEmpty) scala.io.StdIn.readLine("There is no related mapping on the databus.\nPlease type path to local mapping file:\n")
      else mappingInformation.head
    }

    inputFormat match {
      case "tsv" =>
        csv.Reader.csv_to_rddTriple(mappingFile, inputFile.pathAsString, '\t', sc = spark.sparkContext)

      case "csv" =>
        val delimiter = {
          if (mappingInformation.length > 1) mappingInformation(1)
          else scala.io.StdIn.readLine("Please type delimiter of CSV file:\n")
        }

        val quotation = {
          if (mappingInformation.length > 1)  mappingInformation(2)
          else scala.io.StdIn.readLine("Please type quote character of CSV file:\n(e.g. ' \" ' for double quoted entries or ' null ' if there's no quotation)\n")
        }

        val delimiterChar = delimiter.toCharArray.apply(0).asInstanceOf[Character]
        val quoteChar = quotation match {
          case "null" => null
          case _ => quotation.toCharArray.apply(0).asInstanceOf[Character]
        }

        csv.Reader.csv_to_rddTriple(mappingFile, inputFile.pathAsString, delimiterChar, quoteChar, spark.sparkContext)
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
