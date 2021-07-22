package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFFormat}
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.lang.{NQuads, QuadLangs}
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.lang.{NTriples, TripleLangs, Turtle}

object QuadsHandler {

  /**
   * read RDF file as RDD[Triple]
   *
   * @param inputFile   rdf file
   * @param inputFormat rdf serialization
   * @param spark       sparkSession
   * @return rdf data as RDD[Triples]
   */
  def readRDF(inputFile: File, inputFormat: String, spark: SparkSession): RDD[Quad] = {

    inputFormat match {
      case "nq" =>
        NQuads.read(spark, inputFile)
      case "trig" =>
        QuadLangs.read(spark, inputFile, Lang.TRIG)
      case "trix" =>
        QuadLangs.read(spark, inputFile, Lang.TRIX)
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
  def writeRDF(tempDir: File, data: RDD[Quad], outputFormat: String, spark: SparkSession): Unit = {

    outputFormat match {
      case "nq" =>
        NQuads.write(data).saveAsTextFile(tempDir.pathAsString)
      case "trig" =>
        QuadLangs.write(spark, data, Lang.TRIG).saveAsTextFile(tempDir.pathAsString)
      case "trix" =>
        QuadLangs.write(spark, data, Lang.TRIX).saveAsTextFile(tempDir.pathAsString)
    }

  }
}
