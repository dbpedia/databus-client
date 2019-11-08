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
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Vector

class tsv_writer_tests extends FlatSpec {

  val testDir = "./src/resources/test/MappingTests/write/"

  val spark = SparkSession.builder()
    .appName(s"Triple reader")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  "spark" should "not quote empty values when writing csv file" in {

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, ""),
      (4, null)
    ))

    val tempDir = testDir.concat("emptyValues")

    df.coalesce(1).write
      .option("format" , "csv")
      .option("delimiter","\t")
      .option("emptyValue","")
      .csv(tempDir)

    FileUtil.unionFiles(File(tempDir), File(tempDir.concat(".tsv")))
  }

  "spark" should "save csv file with header" in {

    val targetFile = File(testDir) / "csvWithHeader.csv"
    val tempDir = targetFile.parent / "temp"
    val header = Vector[String]("resource", "type", "lat", "long", "seeAlso", "label", "birthPlace", "homepage")

    var rdd = spark.sparkContext
      .parallelize(
        Seq[Seq[String]](
          Seq[String](
          "http://dbpedia.org/resource/Jamaica",
          "http://schema.org/Country",
          "17.9833",
          "-76.8",
          "",
          "Giamaica",
          "",
          "http://jis.gov.jm/"
        ),
            Seq[String](
            "http://dbpedia.org/resource/Bob_Marley",
            "http://xmlns.com/foaf/0.1/Person",
            "",
            "",
            "http://dbpedia.org/resource/Rastafari",
            "Bob Marley",
            "http://dbpedia.org/resource/Jamaica",
            ""
          )
        )
      )

    val sql = spark.sqlContext
    import sql.implicits._

    val ds = sql.createDataset(rdd)
    ds.

    rdd.foreach(println(_))
    ds.show(false)

    ds.coalesce(1).write
      .option("delimiter", "\t")
      .option("emptyValue","")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)

    FileUtil.unionFiles(tempDir, targetFile)
  }

  "rdd[Triple]" should "be saved to tsv and belonging Sparql Query to read it back to RDD[Triple] should be written as well" in {

    val inputFile = File(s"${testDir}testBob.ttl")

    val targetFile: File = inputFile.parent / inputFile.nameWithoutExtension.concat(".tsv")
    val tempDir = inputFile.parent / "temp"
    val headerTempDir = inputFile.parent / "tempheader"
    if (tempDir.exists) tempDir.delete()
    if(headerTempDir.exists) headerTempDir.delete()

    val triplesRDD= RDF_Reader.readRDF(spark,inputFile)

    TTLWriter2.convertToTSV(triplesRDD, spark, targetFile)
  }

}

object TTLWriter2 {

  def convertToTSV(data: RDD[Triple], spark: SparkSession, targetFile:File): File = {


    var tarqlBindings = Seq.empty[String]
    var tarqlConstruct = Seq.empty[String]

    val tempDir = targetFile.parent / "temp"
    val headerTempDir = targetFile.parent / "tempheader"
    if (tempDir.exists) tempDir.delete()
    if(headerTempDir.exists) headerTempDir.delete()

    val prefixPre = "xxx"

    val sql = spark.sqlContext
    import sql.implicits._

    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = data.groupBy(triple => triple.getPredicate.getURI).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?

    var headerVector = Vector[String]("resource")
    var mappingPredicates = Vector[Vector[String]](Vector[String]("resource"))

    allPredicates.collect.foreach(pre => {

      val split = getSplitPredicate(pre:String)
      mappingPredicates = mappingPredicates :+ Vector((split._2),(split._1), s"$prefixPre${split._2}")

      headerVector = headerVector :+ split._2
    })

    var tarqlPrefixes = Seq[String]("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>")
    mappingPredicates.foreach(pre => {
      if (pre.length == 3) tarqlPrefixes= tarqlPrefixes :+ (s"PREFIX ${pre(2)}: <${pre(1)}>")
    })
    println(tarqlPrefixes)


    val triplesTSV = triplesGroupedBySubject.collect().map(allTriplesOfSubject => {
      val result = convertAllTriplesOfSubjectToTSV(allTriplesOfSubject,headerVector, mappingPredicates, tarqlPrefixes)
      tarqlBindings = tarqlBindings ++ result._2
      tarqlConstruct = tarqlConstruct ++ result._3
      result._1
    })

    println("HEADER")
    println(headerVector)
    println(triplesTSV)
    triplesTSV.foreach(println(_))
    val triplesDS = sql.createDataset(triplesTSV)
    val triplesDF = triplesDS.select((0 until mappingPredicates.size).map(r => triplesDS.col("value").getItem(r)): _*)

    val headerDS = sql.createDataset(Vector((headerVector)))
    val headerDF = headerDS.select((0 until mappingPredicates.size).map(r => headerDS.col("value").getItem(r)): _*)

    headerDF.show(false)
    triplesDF.show(false)

    println("FINAL BINDINGS")
    tarqlBindings.distinct.foreach(println(_))
    println("FINAL CONSTRUCTSTRING")
    tarqlConstruct.distinct.foreach(println(_))

    triplesDF.coalesce(1).write
      .option("delimiter", "\t")
      .option("emptyValue","")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)

    headerDF.coalesce(1).write
      .option("delimiter", "\t")
      .csv(headerTempDir.pathAsString)

    FileUtil.unionFilesWithHeaderFile(headerTempDir, tempDir, targetFile)

    TTLWriter2.createTarqlMapFile(tarqlPrefixes, tarqlConstruct, tarqlBindings, targetFile)
  }

  def getSplitPredicate(pre:String): (String, String)={
    val lastHashkeyIndex = pre.lastIndexOf("#")
    val lastSlashIndex = pre.lastIndexOf("/")
    var split = ("","")

    if (lastHashkeyIndex >= lastSlashIndex) split =pre.splitAt(pre.lastIndexOf("#")+1)
    else split =pre.splitAt(pre.lastIndexOf("/")+1)

    split
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

  def createTarqlMapFile(tarqlPrefixes: Seq[String], tarqlConstruct: Seq[String], tarqlBindings: Seq[String], tsvFile:File):File ={
    //    ,
    val prefixStr = tarqlPrefixes.filter(str => !(str.contains("type") && str.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))).mkString("", "\n", "")

    val typeConstruct = tarqlConstruct.distinct.find(str => str.contains("type:type")).get.split(" ")
    val updatedTypeConstruct = typeConstruct.updated(1, "a").mkString(" ")
    //    val constructStr = tarqlConstruct.distinct.updated(tarqlConstruct.indexOf(tarqlConstruct.find(str => str.contains("type:type")).get),updatedTypeConstruct).mkString("", "\n", "\n")

    val constructStrPart = tarqlConstruct.distinct.map(x => x.split(" ").updated(0,"\t").mkString(" ")).filter(str => !str.contains("type:type")).mkString("", "\n", "")

    println(tarqlConstruct.distinct.map(x => x.split(" ").drop(1).mkString(" ")).filter(str => !str.contains("type:type")).mkString("", "\n", ""))
    val constructStr = updatedTypeConstruct.concat(s"\n$constructStrPart")


    val bindingStr = tarqlBindings.distinct.mkString("\t", "\n\t", "")

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

    tarqlFile
  }

  def buildTarqlConstructStr(mappedPredicates:Vector[Vector[String]],predicate:String, bindedSubject:String, bindedPre:String): String ={
    val predicateVec =mappedPredicates.find(vector => vector.contains(predicate)).get
    s"?$bindedSubject ${predicateVec(2)}:${predicate} ?$predicate$bindedPre;"
  }

}