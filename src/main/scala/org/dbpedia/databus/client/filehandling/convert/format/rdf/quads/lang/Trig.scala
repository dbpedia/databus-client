package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.lang

import better.files.File
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.convert.format.rdf.RDFLang

import scala.collection.JavaConverters.asScalaIteratorConverter

object Trig extends RDFLang[RDD[Quad]]{

  override def read(source: String)(implicit sc: SparkContext): RDD[Quad] = {
    val data = RDFDataMgr.loadDatasetGraph(source, Lang.TRIG).find().asScala.toSeq
    sc.parallelize(data)
  }

  override def write(t: RDD[Quad])(implicit sc: SparkContext): File = {
    Trix.convert(t, Lang.TRIG)
  }

}
