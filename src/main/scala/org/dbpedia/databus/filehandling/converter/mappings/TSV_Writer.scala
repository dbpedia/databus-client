package org.dbpedia.databus.filehandling.converter.mappings

import java.io.PrintWriter

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import parquet.io.RecordConsumerLoggingWrapper

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

//  ======================================================


  def convertToTSV(data: RDD[Triple], spark: SparkSession, createMappingFile:Boolean): (DataFrame, DataFrame)={

//    val tempDir = targetFile.parent / "temp"
//    if (tempDir.exists) tempDir.delete()

    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = data.groupBy(triple => triple.getPredicate.getURI).map(_._1)

    val prefixPre = "xxx"

    val mappedPredicates =
      Seq(Seq("resource")) ++ allPredicates.map(
        pre => {
          val split = getSplitPredicate(pre:String)
          Seq(split._2, s"$prefixPre${split._2}", split._1)
        }
      ).collect().sortBy(_.head)

    var tarqlPrefixes:Seq[String] = Seq("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>") ++ mappedPredicates.filter(pre => pre.length==3).map(pre => s"PREFIX ${pre(1)}: <${pre(2)}>")

    val tsv_Data = triplesGroupedBySubject
      .map(allTriplesOfSubject =>
        convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, mappedPredicates)).collect()

    val fields: Seq[StructField] = mappedPredicates.map(prepPre => StructField(prepPre.head, StringType, nullable = true))
    val schema: StructType = StructType(fields)

    schema.foreach(println(_))

    val triplesDF = spark.createDataFrame(spark.sparkContext.parallelize(tsv_Data.map(data => data._1)),schema)

    triplesDF.show(false)

    val schema_mapping:StructType = StructType(
      Seq(
        StructField("prefixes", StringType, true),
        StructField("constructs",StringType,true),
        StructField("bindings",StringType,true)
      )
    )

    println("read out TarqlData")
    val tarqlDF:DataFrame =  spark.createDataFrame(spark.sparkContext.parallelize(tsv_Data.flatMap(data => data._2)), schema_mapping)

    tarqlDF.show()

    (triplesDF, tarqlDF)
  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], predicates:Seq[Seq[String]]): (Row,Seq[Row]) = {
    val bindedPre = "binded"

    var TSVseq: IndexedSeq[String] = IndexedSeq.fill(predicates.size){new String}
    TSVseq = TSVseq.updated(0,triples.last.getSubject.getURI)

    val bindedSubject = predicates.head.head.concat(bindedPre)

    var tarqlSeq:Seq[Seq[String]] =  Seq(Seq("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>", "", s"BIND(URI(?${predicates.head.head}) AS ?$bindedSubject)"))

    triples.foreach(triple => {
      var nonEmpty = false
      var tripleObject = ""

      val triplePredicate = getSplitPredicate(triple.getPredicate.getURI)._2

      println(s"Triple: $triple")
      println(s"triplepre: $triplePredicate")

      if (predicates.exists(seq => seq.contains(triplePredicate))) {
        nonEmpty = true


        var tarqlPart:Seq[String]  = Seq(predicates.filter(pre => pre(0) == triplePredicate).map(pre => s"PREFIX ${pre(1)}: <${pre(2)}>").last)

        if (triple.getObject.isLiteral) {
          val datatype = getSplitPredicate(triple.getObject.getLiteralDatatype.getURI)._2

          if( datatype == "langString") tarqlPart = tarqlPart :+ s"?$bindedSubject ${predicates.find(seq => seq.contains(triplePredicate)).get(1)}:${triplePredicate} ?$triplePredicate;" :+ ""
          else tarqlPart = tarqlPart :+ buildTarqlConstructStr(predicates ,triplePredicate, bindedSubject, bindedPre) :+ s"BIND(xsd:$datatype(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"

          println(s"DATATYPE: $datatype")
          println(s"tarqlPart $tarqlPart")
          tripleObject = triple.getObject.getLiteralLexicalForm
        }
        else if (triple.getObject.isURI) {
          tarqlPart = tarqlPart :+ buildTarqlConstructStr(predicates ,triplePredicate, bindedSubject, bindedPre) :+ s"BIND(URI(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"
          tripleObject = triple.getObject.getURI
        }
        else {
          tripleObject = triple.getObject.getBlankNodeLabel
        }

        tarqlSeq = tarqlSeq :+ tarqlPart
      }


      val index = predicates.indexOf(predicates.find(seq => seq.contains(triplePredicate)).get)

      if (nonEmpty == true) {
        TSVseq = TSVseq.updated(index, tripleObject)
      }
      else {
        TSVseq = TSVseq.updated(index, "")
      }
    })

    println(s"TripleSubj: ${triples.last.getSubject.toString()}")

    println(s"TSVSEQ: $TSVseq")
    println("TARQLSEQ:")
    tarqlSeq.foreach(println(_))

    (Row.fromSeq(TSVseq),tarqlSeq.map(seq => Row.fromSeq(seq)))
  }

  def getSplitPredicate(pre:String): (String, String)={
    val lastHashkeyIndex = pre.lastIndexOf("#")
    val lastSlashIndex = pre.lastIndexOf("/")
    var split = ("","")

    if (lastHashkeyIndex >= lastSlashIndex) split =pre.splitAt(pre.lastIndexOf("#")+1)
    else split =pre.splitAt(pre.lastIndexOf("/")+1)

    split
  }

  def buildTarqlConstructStr(mappedPredicates:Seq[Seq[String]],predicate:String, bindedSubject:String, bindedPre:String): String ={
    val predicateSeq =mappedPredicates.find(seq => seq.contains(predicate)).get
    s"?$bindedSubject ${predicateSeq(1)}:${predicate} ?$predicate$bindedPre;"
  }

  def createTarqlMapFile(tarqlDF:DataFrame, tsvFile:File):File ={
    val cols = tarqlDF.columns
    cols.foreach(println(_))
    tarqlDF.foreach(println(_))

//    println(s"Prefixes ${tarqlDF.select(col = "prefixes").distinct().collect().map(row => row.mkString("", "\n", "")).filter(str => !(str.contains("type")))}")
    val prefixesSeq = tarqlDF.select(col = "prefixes").distinct().collect().map(row => row.mkString("", "\n", "")).filter(str => !(str.contains("type") && str.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#")))
    val prefixesStr = prefixesSeq.mkString("", "\n", "")

    val tarqlConstruct = tarqlDF.select("constructs").distinct().collect().map(row => row.mkString("", "\n", ""))

    val typeConstruct = tarqlConstruct.find(str => str.contains("type:type")).get.split(" ")
    val updatedTypeConstructStr = typeConstruct.updated(1, "a").mkString(" ")
    val constructStrPart = tarqlConstruct.map(x => x.split(" ").updated(0,"\t").mkString(" ")).filter(str => !str.contains("type:type")).mkString("", "\n", "")

    val constructStr = updatedTypeConstructStr.concat(s"\n$constructStrPart")

//    println(tarqlConstruct.distinct.map(x => x.split(" ").drop(1).mkString(" ")).filter(str => !str.contains("type:type")).mkString("", "\n", ""))

    val bindingsSeq = tarqlDF.select("bindings").distinct().collect().map(row => row.mkString("", "\n", ""))
    bindingsSeq.foreach(x => println(s"LINE ${x}"))
    val bindingStr = bindingsSeq.filter(x => x != "").mkString("\t", "\n\t", "")

    val tarqlMappingString =
      s"""$prefixesStr
         |
         |CONSTRUCT {
         |$constructStr}
         |WHERE {
         |$bindingStr
         |}
       """.stripMargin

    val tarqlFile = tsvFile.parent / tsvFile.nameWithoutExtension(true).concat("_mapping").concat(".sparql")

    new PrintWriter(tarqlFile.pathAsString) { write(tarqlMappingString); close }

    tarqlFile
  }

}
