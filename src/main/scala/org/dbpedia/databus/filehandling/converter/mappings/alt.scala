package org.dbpedia.databus.filehandling.converter.mappings

class alt {


  //  def convertToTSV(data: RDD[Triple], spark: SparkSession): RDD[String]={
  //
  //    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
  //    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
  //    val allPredicates = data.groupBy(triple => triple.getPredicate.toString()).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?
  //
  //    val allPredicateVector = allPredicates.collect.toVector
  //
  //    val triplesTSV = triplesGroupedBySubject.map(allTriplesOfSubject => convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, allPredicateVector))
  //
  //    val headerString = "resource\t".concat(allPredicateVector.mkString("\t"))
  //    val header = spark.sparkContext.parallelize(Seq(headerString))
  //
  //    val TSV_RDD = header ++ triplesTSV
  //
  //    TSV_RDD.sortBy(_(1), ascending = false)
  //
  //    return TSV_RDD
  //  }
  //
  //  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], allPredicates: Vector[String]): String={
  //    var TSVstr = triples.last.getSubject.toString
  //
  //    allPredicates.foreach(predicate => {
  //      var alreadyIncluded=false
  //      var tripleObject = ""
  //
  //      triples.foreach(z => {
  //        val triplePredicate = z.getPredicate.toString()
  //        if(predicate == triplePredicate) {
  //          alreadyIncluded = true
  //          tripleObject = z.getObject.toString()
  //        }
  //      })
  //
  //      if(alreadyIncluded == true) {
  //        TSVstr = TSVstr.concat(s"\t$tripleObject")
  //      }
  //      else{
  //        TSVstr = TSVstr.concat("\t")
  //      }
  //    })
  //
  //    return TSVstr
  //  }

}
