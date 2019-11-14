package org.dbpedia.databus.filehandling.converter.rdf_reader

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}

import better.files.File
import org.apache.spark.sql.SparkSession
//import com.google.common.collect.Streams
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.Triple
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object NTriple_Reader {

  //  def readNTriples(spark: SparkSession, inputFile: File): RDD[Triple] = {
  //    NTripleReader.load(spark, inputFile.pathAsString, ErrorParseMode.SKIP, WarningParseMode.IGNORE, checkRDFTerms = false, LoggerFactory.getLogger("ErrorlogReadTriples"))
  //  }

  def read(spark: SparkSession, inputFile: File): RDD[Triple] = {

    val sc = spark.sparkContext
    val rdd = sc.textFile(inputFile.pathAsString, 20)

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
}
