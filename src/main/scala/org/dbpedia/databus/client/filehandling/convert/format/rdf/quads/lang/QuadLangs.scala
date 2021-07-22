package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.lang

import better.files.File
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.compose.Union
import org.apache.jena.graph.{Graph, NodeFactory, Triple}
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.sparql.core.{DatasetGraph, DatasetGraphSink, Quad}
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, InputStream, SequenceInputStream}
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}
import scala.io.{Codec, Source}

object QuadLangs {

  def read(spark: SparkSession, inputFile: File, lang: Lang): RDD[Quad] = {
    val data = RDFDataMgr.loadDatasetGraph(inputFile.pathAsString, lang).find().asScala.toSeq

    val sc = spark.sparkContext
    sc.parallelize(data)
  }

  def write(spark: SparkSession, data: RDD[Quad], lang: Lang): RDD[String] = {
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

    spark.sparkContext.parallelize(Seq(rdf_string))
  }

  def convertIteratorToGraph(graph: Iterable[Quad]): DatasetGraph = {
    val datasetGraph: DatasetGraph = DatasetFactory.create().asDatasetGraph()

    graph.foreach(quad => datasetGraph.add(quad))

    datasetGraph
  }
}