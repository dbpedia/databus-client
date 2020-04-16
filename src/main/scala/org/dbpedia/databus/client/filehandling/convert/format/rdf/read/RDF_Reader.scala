package org.dbpedia.databus.client.filehandling.convert.format.rdf.read

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDF_Reader {

  def read(spark: SparkSession, inputFile: File): RDD[Triple] = {

    val sc = spark.sparkContext
    val statements = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements()
    var data:Seq[Triple] = Seq.empty

    while (statements.hasNext) {
      data = data :+ statements.nextStatement().asTriple()
    }

    sc.parallelize(data)
  }

}