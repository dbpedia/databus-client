package org.dbpedia.databus.filehandling.convert.format.rdf

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.filehandling.convert.format.csv.Writer.writeTriples
import org.dbpedia.databus.filehandling.convert.format.csv
import org.dbpedia.databus.filehandling.convert.format.rdf.read.{NTriple_Reader, RDF_Reader, TTL_Reader}
import org.dbpedia.databus.filehandling.convert.format.rdf.write.{JSONLD_Writer, NTriple_Writer, RDF_Writer, TTL_Writer}

object RDFHandler {

  def readRDF(inputFile: File, inputFormat: String, spark: SparkSession): RDD[Triple] = {

    inputFormat match {
      case "nt" =>
        NTriple_Reader.read(spark, inputFile)

      case "rdf" =>
        RDF_Reader.read(spark, inputFile)

      case "ttl" =>
        //wie geht das besser?
        try {
          val data = NTriple_Reader.read(spark, inputFile)
          data.isEmpty()
          data
        }
        catch {
          case _: org.apache.spark.SparkException => TTL_Reader.read(spark, inputFile)
        }

      case "jsonld" =>
        RDF_Reader.read(spark, inputFile) //Ein Objekt pro Datei

//      case "jsonl" =>
//        try { //Mehrere Objekte pro Datei
//          JSONL_Reader.readJSONL(spark, inputFile)
//        } catch {
//          case _: SparkException =>
//            println("Json Object ueber mehrere Zeilen")
//            RDF_Reader.read(spark, inputFile)
//        }
    }
  }

  def writeRDF(tempDir: File, data: RDD[Triple], outputFormat: String, spark: SparkSession): Unit = {

    outputFormat match {
      case "nt" =>
        NTriple_Writer.convertToNTriple(data).saveAsTextFile(tempDir.pathAsString)

      case "ttl" =>
        TTL_Writer.convertToTTL(data, spark).coalesce(1).saveAsTextFile(tempDir.pathAsString)

      case "jsonld" =>
        RDF_Writer.convertToRDF(data, spark, RDFFormat.JSONLD_PRETTY).saveAsTextFile(tempDir.pathAsString)

      case "rdfxml" =>
        RDF_Writer.convertToRDF(data, spark, RDFFormat.RDFXML).coalesce(1).saveAsTextFile(tempDir.pathAsString)
    }

  }
}
