package mapping

import java.io.PrintWriter
import java.util

import better.files.File
import org.antlr.v4.runtime.atn.SemanticContext.Predicate
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.filehandling.converter.mappings.TSV_Writer
import org.dbpedia.databus.filehandling.converter.rdf_reader.{RDF_Reader, TTL_Reader}
import org.scalatest.FlatSpec
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Vector

class tsv_writer_tests extends FlatSpec {


  "rdd[Triple]" should "be saved to tsv and belonging Sparql Query to read it back to RDD[Triple] should be written as well" in {
    //"/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/writeTSV/testBob.ttl"
    val inputFile = File("./src/resources/test/MappingTests/writeTSV/testBob.ttl")
    val tempDir = inputFile.parent / "temp"
    if (tempDir.exists) tempDir.delete()
    val headerTempDir = inputFile.parent / "tempheader"
    if(headerTempDir.exists) headerTempDir.delete()
    val targetFile: File = inputFile.parent / inputFile.nameWithoutExtension.concat(".tsv")

    val spark = SparkSession.builder()
      .appName(s"Triple reader")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext

    //    val triplesRDD = TTL_Reader.readTTL(spark, inputFile)

    val triplesRDD= RDF_Reader.readRDF(spark,inputFile)
    val solution = TTLWriter.convertToTSV(triplesRDD, spark)
    solution(1).write.option("delimiter", "\t").option("nullValue", "?").option("treatEmptyValuesAsNulls", "true").csv(tempDir.pathAsString)
    solution(0).write.option("delimiter", "\t").csv(headerTempDir.pathAsString)


    TTLWriter.createTarqlMapFile(targetFile)
    FileUtil.unionFilesWithHeaderFile(headerTempDir, tempDir, targetFile)
  }

}

object TTLWriter {

  var tarqlBindings = Seq.empty[String]
  var tarqlConstruct = Seq.empty[String]
  var tarqlPrefixes = Seq[String]("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>")

  def getSplitPredicate(pre:String): (String, String)={
    val lastHashkeyIndex = pre.lastIndexOf("#")
    val lastSlashIndex = pre.lastIndexOf("/")
    var split = ("","")

    if (lastHashkeyIndex >= lastSlashIndex) split =pre.splitAt(pre.lastIndexOf("#")+1)
    else split =pre.splitAt(pre.lastIndexOf("/")+1)

    split
  }


  def convertToTSV(data: RDD[Triple], spark: SparkSession): Vector[DataFrame] = {
    val prefixPre = "xxx"
    val sql = spark.sqlContext

    import sql.implicits._
    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = data.groupBy(triple => triple.getPredicate.getURI).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?

    var headerVector = Vector[String]("resource")
    var mappingPredicates = Vector[Vector[String]](Vector[String]("resource"))

    allPredicates.collect.foreach(pre => {

      val split = getSplitPredicate(pre:String)
      mappingPredicates = mappingPredicates :+ Vector((split._2),(split._1), s"$prefixPre${split._2}")

      headerVector = headerVector :+ split._2
    })

    //      println("MAPPINGVECTOR")
    //      mappingPredicates.foreach(x => x.foreach(println(_)))


    mappingPredicates.foreach(pre => {
      if (pre.length == 3) tarqlPrefixes= tarqlPrefixes :+ (s"PREFIX ${pre(2)}: <${pre(1)}>")
    })
    println(tarqlPrefixes)


    //      var tarqlConstruct = Seq.empty[String]



    val triplesTSV = triplesGroupedBySubject.map(allTriplesOfSubject => {
      val result = convertAllTriplesOfSubjectToTSV(allTriplesOfSubject,headerVector, mappingPredicates, tarqlPrefixes)
      tarqlBindings = tarqlBindings ++ result._2
      tarqlConstruct = tarqlConstruct ++ result._3
      result._1
    })

    val triplesDS = sql.createDataset(triplesTSV)
    val triplesDF = triplesDS.select((0 until mappingPredicates.size).map(r => triplesDS.col("value").getItem(r)): _*)
    val headerDS = sql.createDataset(Vector((headerVector)))
    val headerDF = headerDS.select((0 until mappingPredicates.size).map(r => headerDS.col("value").getItem(r)): _*)

    headerDF.show(false)
    triplesDF.show(false)


//    println("FINAL BINDINGS")
//    println(tarqlBindings.distinct)
//    tarqlBindings.distinct.foreach(println(_))
//    println("FINAL CONSTRUCTSTRING")
//    println(tarqlConstruct.distinct)
//    tarqlConstruct.distinct.foreach(println(_))

    import spark.implicits._

    Vector(headerDF, triplesDF)
  }

  def createTarqlMapFile(tsvFile:File) ={
//    tarqlPrefixes: Seq[String], tarqlConstruct: Seq[String], tarqlBindings: Seq[String],
    val prefixStr = tarqlPrefixes.filter(str => !(str.contains("type") && str.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))).mkString("", "\n", "\n")

    val typeConstruct = tarqlConstruct.distinct.find(str => str.contains("type:type")).get.split(" ")
    val updatedTypeConstruct = typeConstruct.updated(1, "a").mkString(" ")
//    val constructStr = tarqlConstruct.distinct.updated(tarqlConstruct.indexOf(tarqlConstruct.find(str => str.contains("type:type")).get),updatedTypeConstruct).mkString("", "\n", "\n")

    val constructStrPart = tarqlConstruct.distinct.map(x => x.split(" ").updated(0,"\t").mkString(" ")).filter(str => !str.contains("type:type")).mkString("", "\n", "\n")

    println(tarqlConstruct.distinct.map(x => x.split(" ").drop(1).mkString(" ")).filter(str => !str.contains("type:type")).mkString("", "\n", "\n"))
    val constructStr = updatedTypeConstruct.concat(s"\n$constructStrPart")


    val bindingStr = tarqlBindings.distinct.mkString("\t", "\n\t", "\n")

    val tarqlMappingString =
      s"""$prefixStr
         |
         |CONSTRUCT {
         |$constructStr
         |}
         |FROM <$tsvFile>
         |WHERE {
         |$bindingStr
         |}
       """.stripMargin

    val tarqlFile = tsvFile.parent / tsvFile.nameWithoutExtension(true).concat("_mapping").concat(".sparql")

    new PrintWriter(tarqlFile.pathAsString) { write(tarqlMappingString); close }
  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], allPredicates: Vector[String], mappingPredicates: Vector[Vector[String]], tarqlPrefixes: Seq[String]): (Seq[String],Seq[String],Seq[String]) = {
    val bindedPre = "binded"

    var TSVseq: IndexedSeq[String] = IndexedSeq.fill(allPredicates.length){new String}
    TSVseq = TSVseq.updated(0,triples.last.getSubject.getURI)

    //      println(TSVseq)
    //      var tarqlBindPart = Seq[String](s"BIND(URI(?${allPredicates(0)}) AS ?${allPredicates(0)}Binded)")
    val bindedSubject = mappingPredicates(0)(0).concat(bindedPre)
    var tarqlBindPart = Seq[String](s"BIND(URI(?${mappingPredicates(0)(0)}) AS ?$bindedSubject)")
    var tarqlConstructPart = Seq.empty[String]//(s"?$tarqlSubject$bindedPre a ?${allPredicates(0)}$bindedPre)")

    triples.foreach(triple => {
      var alreadyIncluded = false
      var tripleObject = ""

      val triplePredicate = getSplitPredicate(triple.getPredicate.getURI)._2

      //          println(mappingPredicates.exists(vector => vector.contains(triplePredicate)))
      //          println(mappingPredicates.find(vector => vector.contains(triplePredicate)).get)
      //          println(mappingPredicates.indexOf(mappingPredicates.find(vector => vector.contains(triplePredicate)).get))
      //          if (allPredicates.contains(triplePredicate)) {
      if (mappingPredicates.exists(vector => vector.contains(triplePredicate))) {
        alreadyIncluded = true

        if (triple.getObject.isLiteral) {
          val datatype = getSplitPredicate(triple.getObject.getLiteralDatatype.getURI)._2
          tarqlBindPart = tarqlBindPart :+ s"BIND(xsd:$datatype(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"
          tarqlConstructPart = tarqlConstructPart :+ buildTarqlConstructStr(mappingPredicates ,triplePredicate, bindedSubject, bindedPre)
          tripleObject = triple.getObject.getLiteralLexicalForm
        }
        else if (triple.getObject.isURI) {
          tarqlConstructPart = tarqlConstructPart :+ buildTarqlConstructStr(mappingPredicates ,triplePredicate, bindedSubject, bindedPre)
          tarqlBindPart = tarqlBindPart :+ s"BIND(URI(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"
          tripleObject = triple.getObject.getURI
        }
        else tripleObject = triple.getObject.getBlankNodeLabel

      }


      val index = mappingPredicates.indexOf(mappingPredicates.find(vector => vector.contains(triplePredicate)).get)
      //          println(index)
      //          val index = allPredicates.indexOf(triplePredicate)

      if (alreadyIncluded == true) {
        TSVseq = TSVseq.updated(index, tripleObject)
      }
      else {
        TSVseq = TSVseq.updated(index, "")
      }
    })

    //      println("TARQLSTRING")
    //      println(tarqlBindPart)
    //
//          println("TARQLCONSTRUCT")
//    tarqlConstructPart.foreach(println(_))
    //      println("TSVSEQ")
    //      println(TSVseq)
    (TSVseq, tarqlBindPart, tarqlConstructPart)
  }

  def buildTarqlConstructStr(mappedPredicates:Vector[Vector[String]],predicate:String, bindedSubject:String, bindedPre:String): String ={
    val predicateVec =mappedPredicates.find(vector => vector.contains(predicate)).get
//    println("added ConstructLine")
//    println(s"?$bindedSubject ${predicateVec(2)}:${predicate} ?$predicate$bindedPre")
    s"?$bindedSubject ${predicateVec(2)}:${predicate} ?$predicate$bindedPre;"
  }

}