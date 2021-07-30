package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.format

import better.files.File
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}

class NQuads extends Format[RDD[Quad]]{

  override def read(source: String): RDD[Quad] = {

    val rdd = Spark.context.textFile(source, 20)

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

   override def write(quads: RDD[Quad]): File ={

    quads.map(quad => {
      val os = new ByteArrayOutputStream()
      RDFDataMgr.writeQuads(os, Iterator[Quad](quad).asJava)
      os.toString.trim
    }).saveAsTextFile(tempDir.pathAsString)

     FileUtil.unionFiles(tempDir, tempDir / "converted.nq")
  }

}
