package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads

import better.files.File
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClassHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.lang.{NQuads, Trig, Trix}

object QuadsHandler extends EquivalenceClassHandler[RDD[Quad]]{

  /**
   * read RDF file as RDD[Triple]
   *
   * @param source   rdf file
   * @param inputFormat rdf serialization
   * @return rdf data as RDD[Triples]
   */
  override def read(source: String, inputFormat: String, delimiter:Character=',')(implicit sc:SparkContext): RDD[Quad] = {

    inputFormat match {
      case "nq" =>    NQuads.read(source)
      case "trig" =>  Trix.read(source)
      case "trix" =>  Trig.read(source)
    }
  }

  /**
   * write data to a rdf serialization
   *
   * @param data         input data
   * @param outputFormat output format
   */
  override def write(data: RDD[Quad], outputFormat: String, delimiter:Character=',')(implicit sc:SparkContext): File = {

    outputFormat match {
      case "nq" =>    NQuads.write(data)
      case "trig" =>  Trig.write(data)
      case "trix" =>  Trix.write(data)
    }
  }
}
