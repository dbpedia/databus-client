package org.dbpedia.databus.filehandling.converter.rdf_writer

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
    val allPredicates = data.groupBy(triple => triple.getPredicate.getURI).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?

    val allPredicateVector = allPredicates.collect.toVector
    val triplesTSV = triplesGroupedBySubject.map(allTriplesOfSubject => convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, allPredicateVector))

    val tsvheader: Vector[String] = "resource" +: allPredicateVector

    val triplesDS = sql.createDataset(triplesTSV)
    val triplesDF = triplesDS.select((0 until tsvheader.size).map(r => triplesDS.col("value").getItem(r)): _*)
    val headerDS = sql.createDataset(Vector((tsvheader)))
    val headerDF = headerDS.select((0 until tsvheader.size).map(r => headerDS.col("value").getItem(r)): _*)
    //    headerDF.show(false)
    triplesDF.show(false)
    val TSV_Solution = Vector(headerDF, triplesDF)

    return TSV_Solution
  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], allPredicates: Vector[String]): Seq[String] = {
    var TSVseq: Seq[String] = Seq(triples.last.getSubject.getURI)

    allPredicates.foreach(predicate => {
      var alreadyIncluded = false
      var tripleObject = ""

      triples.foreach(triple => {
        val triplePredicate = triple.getPredicate.getURI
        if (predicate == triplePredicate) {
          alreadyIncluded = true
          tripleObject = {
            if (triple.getObject.isLiteral) triple.getObject.getLiteralLexicalForm // triple.getObject.getLiteralDatatype)
            else if (triple.getObject.isURI) triple.getObject.getURI
            else triple.getObject.getBlankNodeLabel
          }
        }
      })

      if (alreadyIncluded == true) {
        TSVseq = TSVseq :+ tripleObject
      }
      else {
        TSVseq = TSVseq :+ ""
      }
    })

    //    println(TSVseq)
    return TSVseq
  }

}
