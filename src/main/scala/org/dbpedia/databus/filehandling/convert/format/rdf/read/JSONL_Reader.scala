package org.dbpedia.databus.filehandling.convert.format.rdf.read

import java.io.ByteArrayInputStream

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JSONL_Reader {

  def readJSONL(spark: SparkSession, inputFile: File): RDD[Triple] = {
    val sc = spark.sparkContext
    val data = sc.textFile(inputFile.pathAsString)
    var tripleRDD = sc.emptyRDD[Triple]

    //    data.foreach(println(_))

    data.foreach(line => {
      println(s"LINE: $line")
      if (!line.trim().isEmpty) {
        println(s"LINE: $line")
        tripleRDD = sc.union(tripleRDD, readJSONLObject(spark, line))
      }
    })

    tripleRDD
  }

  def readJSONLObject(spark: SparkSession, line: String): RDD[Triple] = {
    //    println(line)
    val sc = spark.sparkContext
    var triples = sc.emptyRDD[Triple]

    val statements = ModelFactory.createDefaultModel().read(new ByteArrayInputStream(line.getBytes), "UTF-8", "JSONLD").listStatements()

    while (statements.hasNext) {
      val triple = statements.nextStatement().asTriple()
      val dataTriple = sc.parallelize(Seq(triple))
      triples = sc.union(triples, dataTriple)
    }

    triples
  }

}
