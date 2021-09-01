package org.dbpedia.databus.client.filehandling.convert.mapping

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.mapping.util.Tarql_Writer

object RDF_Triples_Mapper {

  def map_to_quads(data:RDD[Triple], graphName:String): RDD[Quad] = {
    if (graphName == "DefaultGraph") data.map(triple => Quad.create(Quad.defaultGraphIRI, triple))
    else data.map(triple => Quad.create(NodeFactory.createURI(graphName), triple))
  }

  /**
   * write out RDF data as tabular structured data file
   * @param data input RDF data
   * @return TSD file
   */
  def map_to_tsd(data:RDD[Triple], createMapping:Boolean): DataFrame ={

    if (createMapping) {
      val tsvData = triplesToTSD(data, createMappingFile = true)
      Tarql_Writer.createTarqlMapFile(tsvData(1), "./target/databus.tmp/mappingFile.sparql")
      tsvData.head
    }
    else {
      triplesToTSD(data, createMappingFile = false).head
    }
  }

  /**
   * converts RDF data (RDD[Triple] to TSD data [DataFrame]
   *
   * @param inData RDF input data
   * @param createMappingFile create a format.mapping file for format.conversion back to RDF
   * @return tabular structured data
   */
  def triplesToTSD(inData: RDD[Triple], createMappingFile: Boolean): Seq[DataFrame] = {

    val triplesGroupedBySubject = inData.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = inData.groupBy(triple => triple.getPredicate.getURI).map(_._1)

    val prefixPre = "xxx" //for format.mapping file

    val mappedPredicates =
      Seq(Seq("resource")) ++ allPredicates.map(
        pre => {
          val split = splitPredicate(pre: String)
          Seq(split._2, s"$prefixPre${split._2}", split._1)
        }
      ).collect().sortBy(_.head)


    val fields: Seq[StructField] = mappedPredicates.map(prepPre => StructField(prepPre.head, StringType, nullable = true))
    val schema: StructType = StructType(fields)


    if (!createMappingFile) {
      val csvRDD =
        triplesGroupedBySubject.map(allTriplesOfSubject =>
          convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, mappedPredicates.map(seq => seq.head)))

      Seq(Spark.session.createDataFrame(csvRDD, schema))
    }
    else{

      val convertedData = triplesGroupedBySubject
        .map(allTriplesOfSubject =>
          convertTriplesToTSVAndCalculateTarql(allTriplesOfSubject, mappedPredicates)).collect()


      val triplesDF = Spark.session.createDataFrame(
        Spark.context.parallelize(
          convertedData.map(data => data._1)), schema)

      //      println("TSV DATAFRAME")
      //      triplesDF.show(false)
      //=================

      //Calculated TARQL DATA
      val schema_mapping: StructType = StructType(
        Seq(
          StructField("prefixes", StringType, nullable = true),
          StructField("constructs", StringType, nullable = true),
          StructField("bindings", StringType, nullable = true)
        )
      )

      val tarqlDF: DataFrame =
        Spark.session.createDataFrame(
          Spark.context.parallelize(convertedData.flatMap(data => data._2)),
          schema_mapping
        ).distinct()

      //==================

      Seq(triplesDF, tarqlDF)
    }

  }

  def splitPredicate(pre: String): (String, String) = {
    val lastHashkeyIndex = pre.lastIndexOf("#")
    val lastSlashIndex = pre.lastIndexOf("/")
    var split = ("", "")

    if (lastHashkeyIndex >= lastSlashIndex) split = pre.splitAt(pre.lastIndexOf("#") + 1)
    else split = pre.splitAt(pre.lastIndexOf("/") + 1)

    split
  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], predicates: Seq[String]): Row = {

    var tsv_line: Seq[String] = Seq.fill(predicates.size) {
      new String
    }
    tsv_line = tsv_line.updated(0, triples.last.getSubject.toString)

    triples.foreach(triple => {
      var predicate_exists = false
      var tripleObject = ""

      val triplePredicate = splitPredicate(triple.getPredicate.getURI)._2

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
      val triplePredicate = splitPredicate(triple.getPredicate.getURI)._2

      if (predicates.exists(seq => seq.contains(triplePredicate))) {
        predicate_exists = true

        var tarqlPart: Seq[String] = Seq(predicates.filter(pre => pre.head == triplePredicate).map(pre => s"PREFIX ${pre(1)}: <${pre(2)}>").last)

        if (triple.getObject.isLiteral) {

          val datatype =
            splitPredicate(
              Option(triple.getObject.getLiteralDatatypeURI).getOrElse("http://www.w3.org/2001/XMLSchema#string")
            )._2

          if (datatype matches "string|langString") {
            tarqlPart = tarqlPart :+ s"?$bindedSubject ${predicates.find(seq => seq.contains(triplePredicate)).get(1)}:$triplePredicate ?$triplePredicate;" :+ ""
          }
          else {
            tarqlPart = tarqlPart :+ Tarql_Writer.buildTarqlConstructStr(predicates, triplePredicate, bindedSubject, bindedPre) :+ s"BIND(xsd:$datatype(?$triplePredicate) AS ?$triplePredicate$bindedPre)"
          }

          tripleObject = triple.getObject.getLiteralLexicalForm
        }
        else if (triple.getObject.isURI) {
          tarqlPart = tarqlPart :+ Tarql_Writer.buildTarqlConstructStr(predicates, triplePredicate, bindedSubject, bindedPre) :+ s"BIND(URI(?$triplePredicate) AS ?$triplePredicate$bindedPre)"

          tripleObject = triple.getObject.getURI
        }
        else {
          tripleObject = triple.getObject.getBlankNodeLabel
        }

        if(tarqlPart.nonEmpty) tarqlSeq = tarqlSeq :+ tarqlPart

      }


      val index = predicates.indexOf(predicates.find(seq => seq.contains(triplePredicate)).get)

      if (predicate_exists) {
        //        println(TSVseq(index))
        if (TSVseq(index).nonEmpty) {
          val old = TSVseq(index)
          TSVseq.updated(index, s"$old $tripleObject")
        }else{
          TSVseq = TSVseq.updated(index, tripleObject)
        }
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


  //  def handleLangString()={
  ////    println(s"""BIND(STRLANG(?$triplePredicate,"${triple.getObject.getLiteralLanguage}") AS ?$triplePredicate$bindedPre)""")
  //    val constructstr= Tarql_Writer.buildTarqlConstructStr(predicates, triplePredicate, bindedSubject, bindedPre)
  //    val countappearancs = tarqlSeq.count(Seq=>Seq.contains(constructstr))
  //
  //    val countBindings = tarqlSeq.count(Seq=> Seq.contains(s"""BIND(STRLANG(?$triplePredicate,"${triple.getObject.getLiteralLanguage}") AS ?$triplePredicate${countappearancs.toString}$bindedPre)"""))
  //
  //    val constructstring ={
  //      if (countappearancs == 0){
  //        constructstr
  //      }
  //      else{
  //        Tarql_Writer.buildTarqlConstructStr(predicates, triplePredicate, bindedSubject, bindedPre, countappearancs)
  //      }
  //    }
  //
  //    println(constructstring)
  //
  //    if (countappearancs == 0){
  //      tarqlPart = tarqlPart :+ constructstring :+ s"""BIND(STRLANG(?$triplePredicate,"${triple.getObject.getLiteralLanguage}") AS ?$triplePredicate$bindedPre)"""
  //    }else{
  //      tarqlPart = Seq.empty
  //    }
  //  }
}
