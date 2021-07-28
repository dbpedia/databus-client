package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.lang

import better.files.File
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.rdf.RDFLang

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}

object NQuads extends RDFLang[RDD[Quad]]{

  override def read(source: String)(implicit sc:SparkContext): RDD[Quad] = {

    val rdd = sc.textFile(source, 20)

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

   override def write(quads: RDD[Quad])(implicit sparkContext: SparkContext): File ={

    quads.map(quad => {
      val os = new ByteArrayOutputStream()
      RDFDataMgr.writeQuads(os, Iterator[Quad](quad).asJava)
      os.toString.trim
    }).saveAsTextFile(tempDir.pathAsString)

    tempDir
  }

}
