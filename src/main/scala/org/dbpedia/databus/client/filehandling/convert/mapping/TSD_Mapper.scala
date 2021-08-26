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
import org.dbpedia.databus.client.filehandling.CompileConfig
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.tsd
import org.dbpedia.databus.client.filehandling.convert.mapping.util.MappingInfo
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.slf4j.LoggerFactory

object TSD_Mapper {

  def map_to_triples(inputFile: File, conf:CompileConfig):RDD[Triple]={
    var triples = Spark.context.emptyRDD[org.apache.jena.graph.Triple]

    if (conf.mapping != "") {
      val mappingInfo = new MappingInfo(conf.mapping, conf.delimiter.toString, conf.quotation.toString)
      triples = readAsTriples(inputFile, conf.inputFormat, mappingInfo)
    }
    else {
      val possibleMappings = QueryHandler.getPossibleMappings(conf.sha)
      breakable {
        possibleMappings.foreach(mapping => {
          val mappingInfo = QueryHandler.getMappingFileAndInfo(mapping)
          triples = readAsTriples(inputFile, conf.inputFormat, mappingInfo)
          if (!triples.isEmpty()) break
        })
      }
    }

    triples
  }

  def readAsTriples(inputFile: File, inputFormat:String, mappingInfo: MappingInfo): RDD[Triple] = {

    inputFormat match {
      case "tsv" =>
        csv_to_rddTriple(mappingInfo.mappingFile, inputFile.pathAsString, "\t")
      case "csv" =>
        csv_to_rddTriple(mappingInfo.mappingFile, inputFile.pathAsString, mappingInfo.delimiter, mappingInfo.quotation)
    }
  }

  def csv_to_rddTriple(mapFile: String, csvFilePath: String = "", delimiter: String = ",", quoteChar: String ="\""): RDD[Triple] = {

    val tarqlQuery = new TarqlParser(mapFile).getResult

    val csvOptions = new CSVOptions()
    if (delimiter != "null") csvOptions.setDelimiter(delimiter.toCharArray.head)
    if (quoteChar != "null") csvOptions.setQuoteChar(quoteChar.toCharArray.head)

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
    Spark.context.parallelize(seq)
  }


  def map_to_quads()={

  }
}
