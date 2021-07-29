package org.dbpedia.databus.client.filehandling.convert.mapping

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.sparql.QueryHandler

import scala.util.control.Breaks.{break, breakable}
import org.apache.jena.graph.Triple
import org.apache.parquet.io.InputFile
import org.dbpedia.databus.client.filehandling.convert.format.tsd
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.slf4j.LoggerFactory

object TSD_Mapper {

  def map_to_triples(spark: SparkSession, inputFile: File, inputFormat:String, sha:String, mapping:String, delimiter:Character, quotation:Character): RDD[Triple]={
    var triples = spark.sparkContext.emptyRDD[org.apache.jena.graph.Triple]

    if (mapping != "") {
      val mappingInfo = new MappingInfo(mapping, delimiter, quotation)
      triples = readAsTriples(inputFile, inputFormat, spark: SparkSession, mappingInfo)
    }
    else {
      val possibleMappings = QueryHandler.getPossibleMappings(sha)
      breakable {
        possibleMappings.foreach(mapping => {
          val mappingInfo = QueryHandler.getMappingFileAndInfo(mapping)
          triples = readAsTriples(inputFile, inputFormat, spark: SparkSession, mappingInfo)
          if (!triples.isEmpty()) break
        })
      }
    }

    triples
  }

  def readAsTriples(inputFile: File, inputFormat: String, spark: SparkSession, mappingInfo: MappingInfo): RDD[Triple] = {

    inputFormat match {
      case "tsv" =>
        csv_to_rddTriple(mappingInfo.mappingFile, inputFile.pathAsString, '\t', sc = spark.sparkContext)
      case "csv" =>
        csv_to_rddTriple(mappingInfo.mappingFile, inputFile.pathAsString, mappingInfo.delimiter, mappingInfo.quotation, spark.sparkContext)
    }
  }

  def csv_to_rddTriple(mapFile: String, csvFilePath: String = "", delimiter: Character = ',', quoteChar: Character = '"', sc: SparkContext): RDD[Triple] = {

    val tarqlQuery = new TarqlParser(mapFile).getResult

    val csvOptions = new CSVOptions()
    csvOptions.setDelimiter(delimiter)
    csvOptions.setQuoteChar(quoteChar)

    println(
      s"""
         |Used CSVOptions:
         |Delimiter:      ${csvOptions.getDelimiter}
         |EscapeCharacter:${csvOptions.getEscapeChar}
         |QuoteCharacter: ${csvOptions.getQuoteChar}
         |Encoding:       ${csvOptions.getEncoding}
       """.stripMargin)

    var seq: Seq[Triple] = Seq.empty

    if (csvOptions != null) {
      val resultSet = csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlQuery).execTriples()
        case _ => TarqlQueryExecutionFactory.create(tarqlQuery, csvFilePath, csvOptions).execTriples()
      }

      import collection.JavaConverters._
      seq = resultSet.asScala.toSeq
    }
    else {
      LoggerFactory.getLogger("read_CSV").error(s"Delimiter: $delimiter not supported")
      println(s"ERROR (read_CSV): Delimiter: $delimiter not supported")
    }

    //seq.foreach(println(_))
    sc.parallelize(seq)
  }


  def map_to_quads()={

  }
}
