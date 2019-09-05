package org.dbpedia.databus.rdf_reader

import java.util.concurrent.{ExecutorService, Executors}

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.{PipedRDFIterator, PipedRDFStream, PipedTriplesStream}
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
    }

    return data
  }

  def readTTL(spark:SparkSession, inputFile:File): RDD[Triple] ={
    // Create a PipedRDFStream to accept input and a PipedRDFIterator to
    // consume it
    // You can optionally supply a buffer size here for the
    // PipedRDFIterator, see the documentation for details about recommended
    // buffer sizes
    val iter: PipedRDFIterator[Triple] = new PipedRDFIterator[Triple]()
    val inputStream: PipedRDFStream[Triple]  = new PipedTriplesStream(iter)

    // PipedRDFStream and PipedRDFIterator need to be on different threads
    val executor: ExecutorService = Executors.newSingleThreadExecutor()

    // Create a runnable for our parser thread
    val  parser: Runnable = new Runnable() {

      @Override
      def run(): Unit={
        // Call the parsing process.
        RDFDataMgr.parse(inputStream, inputFile.pathAsString)
      }
    }

    // Start the parser on another thread
    executor.submit(parser)

    // We will consume the input on the main thread here

    // We can now iterate over data as it is parsed, parsing only runs as
    // far ahead of our consumption as the buffer size allows
    val sc = spark.sparkContext
    var data = sc.emptyRDD[Triple]


    while (iter.hasNext()) {
      data = sc.union(data, sc.parallelize(Seq(iter.next)))
      data.foreach(println(_))
    }

    inputStream.finish()
    executor.shutdown()
    return data
  }

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

