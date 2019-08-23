package org.dbpedia.databus.rdf_writer

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Vector

object TSV_Writer {

  def convertToTSV(data: RDD[Triple], spark: SparkSession): Vector[DataFrame] = {
    val sql = spark.sqlContext

    import sql.implicits._
    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = data.groupBy(triple => triple.getPredicate.toString()).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?

    val allPredicateVector = allPredicates.collect.toVector
    val triplesTSV = triplesGroupedBySubject.map(allTriplesOfSubject => convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, allPredicateVector))

    val tsvheader: Vector[String] = "resource" +: allPredicateVector
    //    tsvheader.foreach(println(_))
    val triplesDS = sql.createDataset(triplesTSV)
    val triplesDF = triplesDS.select((0 until tsvheader.size).map(r => triplesDS.col("value").getItem(r)): _*)

    val headerDS = sql.createDataset(Vector((tsvheader)))
    headerDS.show(false)
    val headerDF = headerDS.select((0 until tsvheader.size).map(r => headerDS.col("value").getItem(r)): _*)
    headerDF.show(false)

    val TSV_Solution = Vector(headerDF, triplesDF)

    return TSV_Solution
  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], allPredicates: Vector[String]): Seq[String] = {
    var TSVseq: Seq[String] = Seq(triples.last.getSubject.toString)

    allPredicates.foreach(predicate => {
      var alreadyIncluded = false
      var tripleObject = s"\t"

      triples.foreach(z => {
        val triplePredicate = z.getPredicate.toString()
        if (predicate == triplePredicate) {
          alreadyIncluded = true
          tripleObject = z.getObject.toString()
        }
      })

      if (alreadyIncluded == true) {
        TSVseq = TSVseq :+ tripleObject
      }
      else {
        TSVseq = TSVseq :+ s"\t"
      }
    })

    return TSVseq
  }

}
