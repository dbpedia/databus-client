package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format

import better.files.File
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.EquivalenceClass

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, SequenceInputStream}
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}

class NTriples extends EquivalenceClass[RDD[Triple]]{

  override def read(source: String)(implicit sc: SparkContext): RDD[Triple] = {

    val rdd = sc.textFile(source, 20)

    rdd.mapPartitions(
      part => {
        val input: InputStream = new SequenceInputStream({
          val i = part.map(s => new ByteArrayInputStream(s.getBytes("UTF-8")))
          i.asJavaEnumeration
        })

        val it = RiotParsers.createIteratorNTriples(input, null)
        new IteratorResourceClosing[Triple](it, input).asScala
      }
    )

  }

   def write(triples: RDD[Triple])(implicit sc: SparkContext): File ={

    triples.map(triple => {
      val os = new ByteArrayOutputStream()
      RDFDataMgr.writeTriples(os, Iterator[Triple](triple).asJava)
      os.toString.trim
    }).saveAsTextFile(tempDir.pathAsString)

    tempDir
  }

}
