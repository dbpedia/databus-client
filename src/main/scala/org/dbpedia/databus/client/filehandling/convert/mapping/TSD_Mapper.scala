package org.dbpedia.databus.client.filehandling.convert.mapping

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.csv.CSVHandler
import org.dbpedia.databus.client.sparql.QueryHandler

import scala.util.control.Breaks.{break, breakable}
import org.apache.jena.graph.Triple
import org.apache.parquet.io.InputFile

object TSD_Mapper {

  def map_to_triples(spark: SparkSession, inputFile: File, inputFormat:String, sha:String, mapping:String, delimiter:Character, quotation:Character): RDD[Triple]={
    var triples = spark.sparkContext.emptyRDD[org.apache.jena.graph.Triple]

    if (mapping != "") {
      val mappingInfo = new MappingInfo(mapping, delimiter, quotation)
      triples = CSVHandler.readAsTriples(inputFile, inputFormat, spark: SparkSession, mappingInfo)
    }
    else {
      val possibleMappings = QueryHandler.getPossibleMappings(sha)
      breakable {
        possibleMappings.foreach(mapping => {
          val mappingInfo = QueryHandler.getMappingFileAndInfo(mapping)
          triples = CSVHandler.readAsTriples(inputFile, inputFormat, spark: SparkSession, mappingInfo)
          if (!triples.isEmpty()) break
        })
      }
    }

    triples
  }

  def map_to_quads()={

  }
}
