package org.dbpedia.databus.rdf_reader

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDF_Reader {

  def readRDF(spark: SparkSession, inputFile:File): RDD[Triple] = {

    val sc = spark.sparkContext
    val statements = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements()
    var data = sc.emptyRDD[Triple]

    while (statements.hasNext()){
      val triple =statements.nextStatement().asTriple()
      val dataTriple = sc.parallelize(Seq(triple))
      data = sc.union(data, dataTriple)
      sc.parallelize(statements)
    }

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
