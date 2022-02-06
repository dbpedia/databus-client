package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.format

import better.files.File

import org.apache.jena.graph.compose.Union
import org.apache.jena.graph.{Graph, NodeFactory, Triple}
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphSink, Quad}
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, InputStream, SequenceInputStream}
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}
import scala.io.{Codec, Source}

class Trix(lang: Lang=Lang.TRIX) extends Format[RDD[Quad]]{

  override def read(source:String): RDD[Quad] = {
    val data = RDFDataMgr.loadDatasetGraph(source, lang).find().asScala.toSeq

    Spark.context.parallelize(data)
  }

  override def write(data: RDD[Quad]): File = {
    val graphs = data
      .groupBy(quad ⇒ quad.getGraph)
      .map(_._2)
      .collect()
      .toSeq

    val dataset:DatasetGraph = DatasetFactory.create().asDatasetGraph()

    val graphSeq = graphs.map(graph => convertIteratorToGraph(graph))

    graphSeq.foreach(graph => {
      val it = graph.find()
      while(it.hasNext) dataset.add(it.next())
    })

    val os = new ByteArrayOutputStream()
    RDFDataMgr.write(os, dataset, lang)

    val rdf_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "")

    Spark.context.parallelize(Seq(rdf_string)).saveAsTextFile(tempDir.pathAsString)

    FileUtil.unionFiles(tempDir, tempDir / "converted.".concat(lang.toString.toLowerCase))
  }

  def convertIteratorToGraph(graph: Iterable[Quad]): DatasetGraph = {
    val datasetGraph: DatasetGraph = DatasetFactory.create().asDatasetGraph()

    graph.foreach(quad => datasetGraph.add(quad))

    datasetGraph
  }
}