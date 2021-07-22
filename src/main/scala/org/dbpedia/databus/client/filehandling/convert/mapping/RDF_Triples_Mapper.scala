package org.dbpedia.databus.client.filehandling.convert.mapping

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.query.QueryExecution
import org.apache.jena.sparql.core.Quad
import org.apache.spark.sql.DataFrame

object RDF_Triples_Mapper {

  def map_to_quads(data:RDD[Triple]): RDD[Quad] ={
    data.map(triple => Quad.create(NodeFactory.createBlankNode(), triple))
  }

  def map_to_tsd(data:RDD[Triple]): DataFrame ={

  }
}
