package org.dbpedia.databus.rdf_reader

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

  //  def convertToJSONLD(data: RDD[Triple]): RDD[String] = {
  //    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
  //    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
  //    val JSONTriples = triplesGroupedBySubject.map(allTriplesOfSubject => convertIterableToJSONLD(allTriplesOfSubject))
  //
  //
  //    return JSONTriples
  //  }



  //  def convertIterableToJSONLD(triples: Iterable[Triple]): String = {
  //
  //    var JSONstr =s"""{"@id": "${triples.last.getSubject.toString}","""
  //    var vectorTriples = Vector(Vector[String]())
  //
  //    triples.foreach(triple => {
  //      val predicate = triple.getPredicate.toString()
  //      val obj = triple.getObject.toString()
  //      var alreadyContainsPredicate = false
  //
  //      var i = 0
  //      vectorTriples.foreach(vector => {
  //        if (vector.contains(predicate)) {
  //          alreadyContainsPredicate = true
  //          vectorTriples = vectorTriples.updated(i, vector :+ obj)
  //        }
  //        i += 1
  //      })
  //
  //      if (alreadyContainsPredicate == false) {
  //        vectorTriples = vectorTriples :+ Vector(predicate, obj)
  //      }
  //    })
  //
  //    //    println("VEKTOR")
  //    //    vectorTriples.foreach(vect=> {
  //    //      vect.foreach(println(_))
  //    //      println("\n")
  //    //    })
  //
  //    vectorTriples.foreach(vector => {
  //      if (vector.nonEmpty) {
  //        JSONstr = JSONstr.concat(s""""${vector(0)}": """) // PRAEDIKAT WIRD OHNE KLAMMERN HINZUGEFUEGT
  //        var k = 0
  //        vector.foreach(element => {
  //          if (k > 0) { // OBJEKTE
  //            if (element.contains("^^")) {
  //              val split = element.split("\\^\\^")
  //              JSONstr = JSONstr.concat(s"""[{"@value":${split(0)},""")
  //              JSONstr = JSONstr.concat(s""""@type":"${split(1)}"}],""")
  //            } else {
  //              JSONstr = JSONstr.concat(s"""["$element"],""")
  //            }
  //          }
  //          k += 1
  //        })
  //      }
  //    })
  //    JSONstr = JSONstr.dropRight(1).concat("},")
  //    return JSONstr
  //  }
}
