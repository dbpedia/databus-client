package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClassHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.lang.{NTriples, RDFXML, Turtle}

object TripleHandler extends EquivalenceClassHandler[RDD[Triple]] {

  /**
   * read RDF file as RDD[Triple]
   *
   * @param source rdf file path
   * @param inputFormat rdf serialization
   * @return rdf data as RDD[Triples]
   */
  override def read(source: String, inputFormat: String, delimiter:Character=',')(implicit sc:SparkContext): RDD[Triple] = {

    inputFormat match {
      case "nt" => NTriples.read(source)
      case "rdf" => RDFXML.read(source)
      case "ttl" =>
        //wie geht das besser?
        try {
          val data = NTriples.read(source)
          data.isEmpty()
          data
        }
        catch {
          case _: org.apache.spark.SparkException => Turtle.read(source)
        }
    }
  }

  /**
   * write data to a rdf serialization
   *
   * @param data         input data
   * @param outputFormat output format
   */
  override def write(data: RDD[Triple], outputFormat: String, delimiter:Character=',')(implicit sc:SparkContext): File = {
    outputFormat match {
      case "nt" => NTriples.write(data)
      case "ttl" => Turtle.write(data)
      case "rdfxml" => RDFXML.write(data)
    }
  }
}
