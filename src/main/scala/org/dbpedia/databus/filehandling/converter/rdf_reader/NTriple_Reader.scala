package org.dbpedia.databus.filehandling.converter.rdf_reader

import better.files.File
import net.sansa_stack.rdf.spark.io.{ErrorParseMode, NTripleReader, WarningParseMode}
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object NTriple_Reader {

  def readNTriples(spark: SparkSession, inputFile: File): RDD[Triple] = {
    val logger = LoggerFactory.getLogger("ErrorlogReadTriples")
    NTripleReader.load(spark, inputFile.pathAsString, ErrorParseMode.SKIP, WarningParseMode.IGNORE, checkRDFTerms = false, logger)
  }

}
