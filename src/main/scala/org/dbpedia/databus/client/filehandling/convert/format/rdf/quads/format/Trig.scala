package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.format

import better.files.File
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format

import scala.collection.JavaConverters.asScalaIteratorConverter

class Trig extends Format[RDD[Quad]]{

  override def read(source: String): RDD[Quad] = {
    new Trix(Lang.TRIG).read(source)
  }

  override def write(t: RDD[Quad]): File = {
    new Trix(Lang.TRIG).write(t)
  }

}
