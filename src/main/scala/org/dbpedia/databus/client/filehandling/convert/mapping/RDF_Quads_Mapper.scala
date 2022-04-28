package org.dbpedia.databus.client.filehandling.convert.mapping

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.spark.sql.functions.{col, lit}
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
    //calculate partly results
    val triplesData = map_to_triples(data)
    val dataFrameForEachGraph = triplesData.map(triplesResult => {
      val dataFrame = RDF_Triples_Mapper.map_to_tsd(triplesResult.graph, createMapping)
      dataFrame.withColumn("graph", lit(triplesResult.graphName))
    })

    //join partly results
    var resultDataFrame = dataFrameForEachGraph.head

    dataFrameForEachGraph.foreach(df => {
      var columns = Seq.empty[String]
      resultDataFrame.columns.foreach(col => {
        if (df.columns.contains(col)) columns = columns:+col
      })
      resultDataFrame=resultDataFrame.join(df, columns, "outer")
    })

    //sort DataFrame
    val columns = resultDataFrame.columns
    val graphColIndex = columns.indexOf("graph")
    val cols = columns.updated(graphColIndex, columns.head).updated(0, "graph").toSeq
    resultDataFrame.select(cols.map(x=>col(x)):_*).sort("graph")
  }
}

