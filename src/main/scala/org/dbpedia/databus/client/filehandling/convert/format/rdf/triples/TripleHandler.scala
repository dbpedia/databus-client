package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClassHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format.{HDT, NTriples, OWL, RDFXML, Turtle}
import org.semanticweb.owlapi.formats
import org.semanticweb.owlapi.formats.{ManchesterSyntaxDocumentFormat, OWLXMLDocumentFormat}
import org.slf4j.LoggerFactory

class TripleHandler extends EquivalenceClassHandler[RDD[Triple]] {

  /**
   * read RDF file as RDD[Triple]
   *
   * @param source rdf file path
   * @param inputFormat rdf serialization
   * @return rdf data as RDD[Triples]
   */
  override def read(source: String, inputFormat: String, baseURL:String=""): RDD[Triple] = {

    val reader = inputFormat match {
      case "nt" => new NTriples()
      case "ttl" => new Turtle()
      case "rdfxml" => new RDFXML()
      case "hdt" => new HDT(baseURL)
      case "owl" | "omn" | "owx" => new OWL()
    }

    reader.read(source)
  }

  /**
   * write data to a rdf serialization
   *
   * @param data         input data
   * @param outputFormat output format
   */
  override def write(data: RDD[Triple], outputFormat: String, baseURL:String): File = {

    val writer = outputFormat match {
      case "nt" => new NTriples()
      case "ttl" => new Turtle()
      case "rdfxml" => new RDFXML()
      case "hdt" => new HDT(baseURL)
      case "owl" => new OWL()
      case "omn" => new OWL(new ManchesterSyntaxDocumentFormat())
      case "owx" => new OWL(new OWLXMLDocumentFormat())
    }

    try{
      writer.write(data)
    } catch {
      case ex:IllegalArgumentException =>
        LoggerFactory.getLogger(getClass).error(ex.toString)
        null
    }
  }
}
