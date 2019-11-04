package org.dbpedia.databus.filehandling.converter.rdf_reader

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}

import better.files.File
import org.apache.jena.ext.com.google.common.collect.Streams
//import com.google.common.collect.Streams
import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.spark.io.{ErrorParseMode, NTripleReader, WarningParseMode}
import org.apache.jena.atlas.iterator.IteratorResourceClosing
//import org.apache.jena.ext.com.google.common.collect.Streams
import org.apache.jena.graph.Triple
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util.stream.Stream
import scala.collection.JavaConverters._
object NTriple_Reader {

  def readNTriples(spark: SparkSession, inputFile: File): RDD[Triple] = {
    NTripleReader.load(spark, inputFile.pathAsString, ErrorParseMode.SKIP, WarningParseMode.IGNORE, checkRDFTerms = false, LoggerFactory.getLogger("ErrorlogReadTriples"))
  }

  def readNTriplesWithoutSansa(spark: SparkSession, inputFile: File):RDD[Triple]={

    val sc = spark.sparkContext
    val rdd = sc.textFile(inputFile.pathAsString, 20)
    var triplesRDD = sc.emptyRDD[Triple]


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
