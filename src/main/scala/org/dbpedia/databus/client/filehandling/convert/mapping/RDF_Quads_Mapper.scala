package org.dbpedia.databus.client.filehandling.convert.mapping

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.spark.sql.DataFrame
import org.dbpedia.databus.client.filehandling.convert.Spark

object RDF_Quads_Mapper {

  def map_to_triples(data:RDD[Quad]):Array[RDD[Triple]]={
    val graphs = data
      .groupBy(quad ⇒ quad.getGraph)
      .map(_._2)
      .collect()

    graphs.map(iterable => {
      var data: Seq[Triple] = Seq.empty
      val iterator = iterable.iterator
      while (iterator.hasNext) {
        data = data :+ iterator.next().asTriple()
      }
      Spark.context.parallelize(data)
    })

  }

  def map_to_tsd(data:RDD[Quad], createMapping:Boolean):Array[DataFrame]={
    val triplesData = map_to_triples(data)
    triplesData.map(graph => RDF_Triples_Mapper.map_to_tsd(graph, createMapping))
  }
}
