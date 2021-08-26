package org.dbpedia.databus.client.filehandling.convert.mapping

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.mapping.util.TriplesResult

object RDF_Quads_Mapper {

  def map_to_triples(data:RDD[Quad]):Array[TriplesResult]={
    val graphs = data
      .groupBy(quad ⇒ quad.getGraph)
      .map(_._2)
      .collect()

    graphs.map(iterable => {
      var data: Seq[Triple] = Seq.empty
      val iterator = iterable.iterator
      var graphName = ""
      while (iterator.hasNext) {
        val quad = iterator.next()
        graphName = quad.getGraph.toString
        data = data :+ quad.asTriple()
      }
      new TriplesResult(graphName, Spark.context.parallelize(data))
    })

  }

  def map_to_tsd(data:RDD[Quad], createMapping:Boolean):DataFrame={
    val triplesData = map_to_triples(data)
    val dataFrameForEachGraph = triplesData.map(triplesResult => {
      val dataFrame = RDF_Triples_Mapper.map_to_tsd(triplesResult.graph, createMapping)
      dataFrame.show()
      dataFrame.withColumn("graph", lit(triplesResult.graphName))
    })

    val resultDataFrame = dataFrameForEachGraph.head

    dataFrameForEachGraph.foreach()
    df1.join(df2, df1.col("column").equalTo(df2("column")))
    dataFrameForEachGraph.reduce(_ join _)
  }
}

