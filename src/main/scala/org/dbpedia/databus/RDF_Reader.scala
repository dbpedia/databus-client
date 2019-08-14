package org.dbpedia.databus

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import better.files.File
import net.sansa_stack.rdf.spark.io.{ErrorParseMode, NTripleReader, WarningParseMode}
import org.apache.commons.validator.routines.UrlValidator
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{RDFDataMgr, RiotNotFoundException}
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.io.Source


object RDF_Reader {

  def readNTriples(spark: SparkSession, inputFile:File): RDD[Triple] = {
    val logger = LoggerFactory.getLogger("ErrorlogReadTriples")
    NTripleReader.load(spark, inputFile.pathAsString, ErrorParseMode.SKIP, WarningParseMode.IGNORE, false, logger)
  }

  //  Erkennt nicht alle URIS (weil sie nicht exisitieren?)
  def readJSONLD(spark: SparkSession, inputFile:File): RDD[Triple] = {
    val sc = spark.sparkContext

    val data = sc.textFile(inputFile.pathAsString)
    var tripleRDD = sc.emptyRDD[Triple]

    try{
      data.foreach(line => tripleRDD = sc.union(tripleRDD, readJSONLDObject(spark, line)))
    } catch {
      case onlyOneJsonObject: SparkException => tripleRDD = readRDF(spark, inputFile)
    }



//    var bracesOpen = 0
//    var jsonString = ""
//
//    for(line <- Source.fromFile(inputFile.toJava).getLines()) {
//      if (line.contains("{")) bracesOpen += 1
//      if (line.contains("}")) bracesOpen -= 1
//      jsonString = jsonString.concat(line)
//      if (bracesOpen == 0) {
//        println(jsonString)
//        tripleRDD = sc.union(readJSONLDObject(spark, jsonString))
//        jsonString = ""
//      }
//    }

    return tripleRDD
  }

  def readJSONLDObject(spark: SparkSession, line: String):RDD[Triple] ={

    val sc = spark.sparkContext
    var triples = sc.emptyRDD[Triple]
    val statements = ModelFactory.createDefaultModel().read(new ByteArrayInputStream(line.getBytes), "UTF-8", "JSONLD").listStatements()
//    val statements = RDFDataMgr.loadModel(new ByteArrayInputStream(line.getBytes)).listStatements()

    while (statements.hasNext()){
      val triple =statements.nextStatement().asTriple()
      val dataTriple = sc.parallelize(Seq(triple))
      triples = sc.union(triples, dataTriple)
    }

    return triples
  }

  def readRDF(spark: SparkSession, inputFile:File): RDD[Triple] = {

    val sc = spark.sparkContext
    val statements = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements()
    var data = sc.emptyRDD[Triple]

//    val schemes = Array("http","https")
//    val urlValidator = new UrlValidator(schemes)

    while (statements.hasNext()){
      val triple =statements.nextStatement().asTriple()
      val dataTriple = sc.parallelize(Seq(triple))
      data = sc.union(data, dataTriple)
    }

//    data.foreach(line => tripleRDD = sc.union(readRDFTriples(spark, line)))

    return data
  }

//  //  Erkennt nicht alle URIS (weil sie nicht exisitieren?)
//  def readJSONLD(spark: SparkSession, inputFile:File): RDD[Triple] = {
//    val sc = spark.sparkContext
//
//    val data = sc.textFile(inputFile.pathAsString)
//    var tripleRDD = sc.emptyRDD[Triple]
//
//
//    var bracesOpen = 0
//    var jsonString = ""
//
//    for(line <- Source.fromFile(inputFile.toJava).getLines()) {
//      if (line.contains("{")) bracesOpen += 1
//      if (line.contains("}")) bracesOpen -= 1
//      jsonString = jsonString.concat(line)
//      if (bracesOpen == 0) {
//        println(jsonString)
//        tripleRDD = sc.union(readJSONLDObject(spark, jsonString))
//        jsonString = ""
//      }
//    }
//
//    return tripleRDD
//  }
//
//  def readJSONLDObject(spark: SparkSession, line: String):RDD[Triple] ={
//
//    val sc = spark.sparkContext
//    var triples = sc.emptyRDD[Triple]
//    val statements = ModelFactory.createDefaultModel().read(new ByteArrayInputStream(line.getBytes), "UTF-8", "JSONLD").listStatements()
//    //    val statements = RDFDataMgr.loadModel(new ByteArrayInputStream(line.getBytes)).listStatements()
//
//    while (statements.hasNext()){
//      val triple =statements.nextStatement().asTriple()
//      val dataTriple = sc.parallelize(Seq(triple))
//      triples = sc.union(triples, dataTriple)
//    }
//
//    return triples
//  }
}

