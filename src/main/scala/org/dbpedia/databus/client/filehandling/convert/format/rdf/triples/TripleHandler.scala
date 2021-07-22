package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.lang.{NTriples, TripleLangs, Turtle}

object TripleHandler {

  /**
   * read RDF file as RDD[Triple]
   *
   * @param inputFile   rdf file
   * @param inputFormat rdf serialization
   * @param spark       sparkSession
   * @return rdf data as RDD[Triples]
   */
  def readRDF(inputFile: File, inputFormat: String, spark: SparkSession): RDD[Triple] = {

    inputFormat match {
      case "nt" =>
        NTriples.read(spark, inputFile)

      case "rdf" =>
        TripleLangs.read(spark, inputFile)

      case "ttl" =>
        //wie geht das besser?
        try {
          val data = NTriples.read(spark, inputFile)
          data.isEmpty()
          data
        }
        catch {
          case _: org.apache.spark.SparkException => Turtle.read(spark, inputFile)
        }

      case "jsonld" =>
        TripleLangs.read(spark, inputFile)
    }
  }

  /**
   * write data to a rdf serialization
   *
   * @param tempDir      target temporary directory
   * @param data         input data
   * @param outputFormat output format
   * @param spark        sparkSession
   */
  def writeRDF(tempDir: File, data: RDD[Triple], outputFormat: String, spark: SparkSession): Unit = {

    outputFormat match {
      case "nt" =>
        NTriples.convertToNTriple(data).saveAsTextFile(tempDir.pathAsString)

      case "ttl" =>
        Turtle.convertToTTL(data, spark).coalesce(1).saveAsTextFile(tempDir.pathAsString)

      case "jsonld" =>
        TripleLangs.convertToRDF(data, spark, RDFFormat.JSONLD_PRETTY).saveAsTextFile(tempDir.pathAsString)

      case "rdfxml" =>
        TripleLangs.convertToRDF(data, spark, RDFFormat.RDFXML).coalesce(1).saveAsTextFile(tempDir.pathAsString)
    }

  }
}
