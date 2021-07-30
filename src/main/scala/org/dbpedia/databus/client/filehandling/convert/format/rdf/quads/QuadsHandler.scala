package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads

import better.files.File
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClassHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.format.{NQuads, Trig, Trix}

class QuadsHandler extends EquivalenceClassHandler[RDD[Quad]]{

  /**
   * read RDF file as RDD[Triple]
   *
   * @param source   rdf file
   * @param inputFormat rdf serialization
   * @return rdf data as RDD[Triples]
   */
  override def read(source: String, inputFormat: String): RDD[Quad] = {

    inputFormat match {
      case "nq" =>    new NQuads().read(source)
      case "trig" =>  new Trix().read(source)
      case "trix" =>  new Trig().read(source)
    }
  }

  /**
   * write data to a rdf serialization
   *
   * @param data         input data
   * @param outputFormat output format
   */
  override def write(data: RDD[Quad], outputFormat: String): File = {

    outputFormat match {
      case "nq" =>   new NQuads().write(data)
      case "trig" => new Trig().write(data)
      case "trix" => new Trix().write(data)
    }

  }
}
