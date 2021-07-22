package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.lang

import better.files.File
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}

object NQuads {

  def read(spark: SparkSession, inputFile: File): RDD[Quad] = {

    val sc = spark.sparkContext
    val rdd = sc.textFile(inputFile.pathAsString, 20)

    rdd.mapPartitions(
      part => {
        val input: InputStream = new SequenceInputStream({
          val i = part.map(s => new ByteArrayInputStream(s.getBytes("UTF-8")))
          i.asJavaEnumeration
        })

        val it = RiotParsers.createIteratorNQuads(input, null)
        new IteratorResourceClosing[Quad](it, input).asScala
      }
    )

  }

  def write(quads: RDD[Quad]): RDD[String] = {

    quads.map(quad => {
      val os = new ByteArrayOutputStream()
      RDFDataMgr.writeQuads(os, Iterator[Quad](quad).asJava)
      os.toString.trim
    })

  }
}
