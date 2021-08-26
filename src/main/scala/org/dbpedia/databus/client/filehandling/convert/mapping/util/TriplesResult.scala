package org.dbpedia.databus.client.filehandling.convert.mapping.util

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD

class TriplesResult(val graphName: String, val graph: RDD[Triple]) {}
