package org.dbpedia.databus.filehandling.converter.mappings

import java.io.PrintWriter

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TSV_Writer {

  def convertToTSV(data: RDD[Triple], spark: SparkSession): DataFrame = {

    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = data.groupBy(triple => triple.getPredicate.getURI).map(_._1)

    val splitPredicates =
      Seq("resource") ++ allPredicates.map(
        pre => getSplitPredicate(pre: String)._2
      ).collect().sortBy(_.head)

    val fields: Seq[StructField] = splitPredicates.map(pre => StructField(pre, StringType, nullable = true))
    val schema: StructType = StructType(fields)

    val tsvRDD = triplesGroupedBySubject.map(allTriplesOfSubject => convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, splitPredicates))

    spark.createDataFrame(tsvRDD, schema)
  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], predicates: Seq[String]): Row = {

    var tsv_line: Seq[String] = Seq.fill(predicates.size) {
      new String
    }
    tsv_line = tsv_line.updated(0, triples.last.getSubject.getURI)

    triples.foreach(triple => {
      var predicate_exists = false
      var tripleObject = ""

      val triplePredicate = getSplitPredicate(triple.getPredicate.getURI)._2

      if (predicates.exists(seq => seq.contains(triplePredicate))) {
        predicate_exists = true

        if (triple.getObject.isLiteral) tripleObject = triple.getObject.getLiteralLexicalForm
        else if (triple.getObject.isURI) tripleObject = triple.getObject.getURI
        else tripleObject = triple.getObject.getBlankNodeLabel
      }


      val index = predicates.indexOf(predicates.find(seq => seq.contains(triplePredicate)).get)

      if (predicate_exists) {
        tsv_line = tsv_line.updated(index, tripleObject)
      }
      else {
        tsv_line = tsv_line.updated(index, "")
      }
    })

    Row.fromSeq(tsv_line)
  }

  //  ======================================================

  def getSplitPredicate(pre: String): (String, String) = {
    val lastHashkeyIndex = pre.lastIndexOf("#")
    val lastSlashIndex = pre.lastIndexOf("/")
    var split = ("", "")

    if (lastHashkeyIndex >= lastSlashIndex) split = pre.splitAt(pre.lastIndexOf("#") + 1)
    else split = pre.splitAt(pre.lastIndexOf("/") + 1)

    split
  }

  def convertToTSV(inData: RDD[Triple], spark: SparkSession, createMappingFile: Boolean): (DataFrame, DataFrame) = {

    val triplesGroupedBySubject = inData.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = inData.groupBy(triple => triple.getPredicate.getURI).map(_._1)

    //TARQL DATA
    val prefixPre = "xxx"

    val mappedPredicates =
      Seq(Seq("resource")) ++ allPredicates.map(
        pre => {
          val split = getSplitPredicate(pre: String)
          Seq(split._2, s"$prefixPre${split._2}", split._1)
        }
      ).collect().sortBy(_.head)
    //==========


    val convertedData = triplesGroupedBySubject
      .map(allTriplesOfSubject =>
        convertTriplesToTSVAndCalculateTarql(allTriplesOfSubject, mappedPredicates)).collect()


    //Calculated TSV DATA
    val fields: Seq[StructField] = mappedPredicates.map(prepPre => StructField(prepPre.head, StringType, nullable = true))
    val schema: StructType = StructType(fields)

    val triplesDF = spark.createDataFrame(spark.sparkContext.parallelize(convertedData.map(data => data._1)), schema)
    println("TSV DATAFRAME")
    triplesDF.show(false)
    //=================

    //Calculated TARQL DATA
    val schema_mapping: StructType = StructType(
      Seq(
        StructField("prefixes", StringType, true),
        StructField("constructs", StringType, true),
        StructField("bindings", StringType, true)
      )
    )

    val tarqlDF: DataFrame =
      spark.createDataFrame(
        spark.sparkContext.parallelize(convertedData.flatMap(data => data._2)),
        schema_mapping
      ).distinct()

    //==================

    (triplesDF, tarqlDF)
  }

  def convertTriplesToTSVAndCalculateTarql(triples: Iterable[Triple], predicates: Seq[Seq[String]]): (Row, Seq[Row]) = {

    //TSV DATA
    var TSVseq: IndexedSeq[String] = IndexedSeq.fill(predicates.size) {
      new String
    }
    TSVseq = TSVseq.updated(0, triples.last.getSubject.getURI)

    //TARQL DATA
    val bindedPre = "binded"
    val bindedSubject = predicates.head.head.concat(bindedPre)
    var tarqlSeq: Seq[Seq[String]] = Seq(Seq("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>", "", s"BIND(URI(?${predicates.head.head}) AS ?$bindedSubject)"))

    triples.foreach(triple => {
      var predicate_exists = false
      var tripleObject = ""

      val triplePredicate = getSplitPredicate(triple.getPredicate.getURI)._2

      if (predicates.exists(seq => seq.contains(triplePredicate))) {
        predicate_exists = true

        var tarqlPart: Seq[String] = Seq(predicates.filter(pre => pre(0) == triplePredicate).map(pre => s"PREFIX ${pre(1)}: <${pre(2)}>").last)

        if (triple.getObject.isLiteral) {
          val datatype = getSplitPredicate(triple.getObject.getLiteralDatatype.getURI)._2

          if (datatype == "langString") tarqlPart = tarqlPart :+ s"?$bindedSubject ${predicates.find(seq => seq.contains(triplePredicate)).get(1)}:${triplePredicate} ?$triplePredicate;" :+ ""
          else tarqlPart = tarqlPart :+ buildTarqlConstructStr(predicates, triplePredicate, bindedSubject, bindedPre) :+ s"BIND(xsd:$datatype(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"

          tripleObject = triple.getObject.getLiteralLexicalForm
        }
        else if (triple.getObject.isURI) {
          tarqlPart = tarqlPart :+ buildTarqlConstructStr(predicates, triplePredicate, bindedSubject, bindedPre) :+ s"BIND(URI(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"

          tripleObject = triple.getObject.getURI
        }
        else {
          tripleObject = triple.getObject.getBlankNodeLabel
        }

        tarqlSeq = tarqlSeq :+ tarqlPart
      }


      val index = predicates.indexOf(predicates.find(seq => seq.contains(triplePredicate)).get)

      if (predicate_exists) {
        TSVseq = TSVseq.updated(index, tripleObject)
      }
      else {
        TSVseq = TSVseq.updated(index, "")
      }
    })
    //
    //    println(s"TripleSubj: ${triples.last.getSubject.toString()}")
    //    println(s"TSVSEQ: $TSVseq")
    //    println("TARQLSEQ:")
    //    tarqlSeq.foreach(println(_))

    (Row.fromSeq(TSVseq), tarqlSeq.map(seq => Row.fromSeq(seq)))
  }

  def buildTarqlConstructStr(mappedPredicates: Seq[Seq[String]], predicate: String, bindedSubject: String, bindedPre: String): String = {
    val predicateSeq = mappedPredicates.find(seq => seq.contains(predicate)).get
    s"?$bindedSubject ${predicateSeq(1)}:${predicate} ?$predicate$bindedPre;"
  }

  def createTarqlMapFile(tarqlDF: DataFrame, file: File) = {
    val cols = tarqlDF.columns

    println("TARQL DATAFRAME")
    tarqlDF.show(false)

    val prefixesSeq =
      tarqlDF.select(cols(0))
        .distinct()
        .collect()
        .map(row => row.mkString(""))
        .filter(str => !(str.contains("type") && str.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#")))

    val prefixesStr = prefixesSeq.mkString("", "\n", "")

    val tarqlConstruct =
      tarqlDF
        .select(cols(1))
        .distinct()
        .collect()
        .map(row => row.mkString(""))


    var updatedTypeConstructStr = ""

    try {
      updatedTypeConstructStr =
        tarqlConstruct
          .find(str => str.contains("type:type"))
          .map(str => str.split(" ").updated(1, "a").mkString(" "))
          .last
    } catch {
      case none:NoSuchElementException => println("NO TYPE IN TRIPLES INCLUDED")
    }

    val constructStrPart =
      tarqlConstruct
        .map(x => x.split(" ").updated(0, "\t").mkString(" "))
        .filter(str => !str.contains("type:type"))
        .mkString("", "\n", "")

    val constructStr = updatedTypeConstructStr.concat(s"\n$constructStrPart")

    val bindingsSeq = tarqlDF
      .select("bindings")
      .distinct()
      .collect()
      .map(row => row.mkString(""))
      .filter(str => !str.isEmpty)

    val bindingStr = bindingsSeq.mkString("\t", "\n\t", "")

    val tarqlMappingString =
      s"""$prefixesStr
         |
         |CONSTRUCT {
         |$constructStr}
         |WHERE {
         |$bindingStr
         |}
       """.stripMargin

    println(s"CALCULATED TARQLSTRING: \n$tarqlMappingString")

    new PrintWriter(file.pathAsString) {
      write(tarqlMappingString); close
    }
  }

}
