package org.dbpedia.databus.client.filehandling.convert.format.rdf.write

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


object NTriple_Writer {

  def convertToNTriple(triples: RDD[Triple]): RDD[String] = {

    triples.map(triple => {
      val os = new ByteArrayOutputStream()
      RDFDataMgr.writeTriples(os, Iterator[Triple](triple).asJava)
      os.toString.trim
    })
  }
}
