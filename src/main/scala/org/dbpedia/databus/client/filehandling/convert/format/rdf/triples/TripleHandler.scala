package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClassHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format.{NTriples, OWL, RDFXML, Turtle}
import org.semanticweb.owlapi.formats
import org.semanticweb.owlapi.formats.{ManchesterSyntaxDocumentFormat, OWLXMLDocumentFormat}

class TripleHandler extends EquivalenceClassHandler[RDD[Triple]] {

  /**
   * read RDF file as RDD[Triple]
   *
   * @param source rdf file path
   * @param inputFormat rdf serialization
   * @return rdf data as RDD[Triples]
   */
  override def read(source: String, inputFormat: String): RDD[Triple] = {

    inputFormat match {
      case "nt" => new NTriples().read(source)
      case "rdfxml" => new RDFXML().read(source)
      case "owl" | "omn" | "owx" => new OWL().read(source)
      case "ttl" =>
        //wie geht das besser?
        try {
          val data = new NTriples().read(source)
          data.isEmpty()
          data
        }
        catch {
          case _: org.apache.spark.SparkException => new Turtle().read(source)
        }
    }
  }

  /**
   * write data to a rdf serialization
   *
   * @param data         input data
   * @param outputFormat output format
   */
  override def write(data: RDD[Triple], outputFormat: String): File = {

    outputFormat match {
      case "nt" => new NTriples().write(data)
      case "ttl" => new Turtle().write(data)
      case "rdfxml" => new RDFXML().write(data)
      case "owl" => new OWL().write(data)
      case "omn" => new OWL(new ManchesterSyntaxDocumentFormat()).write(data)
      case "owx" => new OWL(new OWLXMLDocumentFormat()).write(data)
    }

  }
}
