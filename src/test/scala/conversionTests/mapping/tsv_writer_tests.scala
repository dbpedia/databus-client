package conversionTests.mapping

import java.io.PrintWriter
import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format.RDFXML
import org.scalatest.FlatSpec

import scala.collection.immutable.Vector

class tsv_writer_tests extends FlatSpec {

  val testDir = "./src/resources/test/MappingTests/write/"

  val spark:SparkSession = SparkSession.builder()
    .appName(s"Triple reader")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  implicit val sc:SparkContext= spark.sparkContext

  "spark" should "not quote empty values when writing csv file" in {

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, ""),
      (4, null)
    ))

    val tempDir = testDir.concat("emptyValues")
    FileUtils.deleteDirectory(File(tempDir).toJava)

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
//
//    val seq1 =          Seq[String](
//      "http://dbpedia.org/resource/Jamaica",
//      "http://schema.org/Country",
//      "17.9833",
//      "-76.8",
//      "",
//      "Giamaica",
//      "",
//      "http://jis.gov.jm/"
//    )

    rdd.foreach(println(_))
    import spark.implicits._

    val df =rdd.toDF()

    df.show(false)


    df.coalesce(1).write
      .option("delimiter", "\t")
      .option("emptyValue","")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)

    FileUtil.unionFiles(tempDir, targetFile)
  }

//  "DataFrame with right column" should "be created from testBob.ttl2" in {
//
//    val inputFile = File(s"${testDir}testBob.ttl")
//    val targetFile: File = inputFile.parent / inputFile.nameWithoutExtension.concat(".tsv")
//    val tempDir = inputFile.parent / "temp"
//    val headerTempDir = inputFile.parent / "tempheader"
//    if (tempDir.exists) tempDir.delete()
//    if(headerTempDir.exists) headerTempDir.delete()
//
//    val triplesRDD = RDF_Reader.readRDF(spark,inputFile)
//
//    TTLWriterTest.convertToTSV(triplesRDD, spark, targetFile)
//
//  }



  def returnCusomDataType():(Row,Seq[Row])={
    val seq1 = Seq[String]("PREFIX xxxtype: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>","?resourcebinded xxxtype:type ?typebinded;","BIND(URI(?type) AS ?typebinded")
    val seq2 = Seq[String]("PREFIX xxxlong: <http://www.w3.org/2003/01/geo/wgs84_pos#>","?resourcebinded xxxlong:long ?longbinded;","BIND(xsd:float(?long) AS ?longbinded)")
    val tsvSeq = Seq[String]("http://dbpedia.org/resource/Jamaica","" , "http://jis.gov.jm/", "Giamaica", "17.9833", "-76.8", "", "http://schema.org/Country")

    (Row.fromSeq(tsvSeq), Seq(Row.fromSeq(seq1), Row.fromSeq(seq2)))
  }


  "right dataframe" should "be created from List of Construct Prefix and BindingsSeq" in {

    val rdd = spark.sparkContext
      .parallelize(
        Seq(
          (returnCusomDataType()._1,
          returnCusomDataType()._2)
        )
      )

    rdd.foreach(println(_))


    val schemaTSV = StructType(
      Seq(
        StructField("resource",StringType,true),
      StructField("birthPlace",StringType,true),
      StructField("homepage",StringType,true),
      StructField("label",StringType,true),
      StructField("lat",StringType,true),
      StructField("long",StringType,true),
      StructField("seeAlso",StringType,true),
      StructField("type",StringType,true)))

    val schema_mapping:StructType = StructType(
      Seq(
        StructField("prefixes", StringType, true),
        StructField("constructs",StringType,true),
        StructField("bindings",StringType,true)
      )
    )

    val df= spark.createDataFrame(rdd.flatMap(tuple => tuple._2), schema_mapping)

    df.show

    val data = spark.createDataFrame(rdd.map(x => x._1), schemaTSV)
//
    data.show()
  }



  "rdd[Triple]" should "be saved to tsv and belonging Sparql Query to read it back to RDD[Triple] should be written as well" in {

    val inputFile = File(s"${testDir}testBob.ttl")
    val targetFile: File = inputFile.parent / inputFile.nameWithoutExtension.concat(".tsv")
    val tempDir = inputFile.parent / "temp"
    val headerTempDir = inputFile.parent / "tempheader"
    if (tempDir.exists) tempDir.delete()
    if(headerTempDir.exists) headerTempDir.delete()

    val triplesRDD= RDFXML.read(inputFile.pathAsString)

    TTLWriter2.convertToTSV(triplesRDD, spark, targetFile)
  }

  "DataFrame with right header" should "be create from testBob.ttl and saved as csv with corresponding mapping file" in {

    val inputFile = File(s"${testDir}testBob.ttl")
    val targetFile: File = inputFile.parent / inputFile.nameWithoutExtension.concat(".tsv")
    val tempDir = inputFile.parent / "temp"
    val headerTempDir = inputFile.parent / "tempheader"
    if (tempDir.exists) tempDir.delete()
    if(headerTempDir.exists) headerTempDir.delete()

    val triplesRDD= RDFXML.read(inputFile.pathAsString)

    TTLWriterTest.convertToTSV(triplesRDD, spark, targetFile, true)
  }

}


object TTLWriterTest {

  def convertToTSV(data: RDD[Triple], spark: SparkSession, targetFile:File, createMappingFile:Boolean):File={

    val tempDir = targetFile.parent / "temp"
    if (tempDir.exists) tempDir.delete()

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
        convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, mappedPredicates, tarqlPrefixes, test=true))

    val fields: Seq[StructField] = mappedPredicates.map(prepPre => StructField(prepPre.head, StringType, nullable = true))
    val schema: StructType = StructType(fields)

    val triplesDF = spark.createDataFrame(tsv_Data.map(_._1),schema)
    triplesDF.show(false)

    triplesDF.coalesce(1).write
      .option("delimiter", "\t")
      .option("emptyValue","")
      .option("header","true")
      .option("treatEmptyValuesAsNulls", "false")
      .csv(tempDir.pathAsString)


    val tarqlConstruct = tsv_Data.map(_._2.toSeq).distinct().collect().flatten.map(x => x.toString)

    val tarqlBindings = tsv_Data.map(_._3.toSeq).distinct().collect().flatten.map(x => x.toString)

    TTLWriterTest.createTarqlMapFile(tarqlPrefixes, tarqlConstruct, tarqlBindings, targetFile)

  }

  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], predicates:Seq[Seq[String]], tarqlPrefixes: Seq[String], test:Boolean): (Row,Row,Row) = {
    val bindedPre = "binded"

//    println(s"schemaSize: ${predicates.size}")
    var TSVseq: IndexedSeq[String] = IndexedSeq.fill(predicates.size){new String}
    TSVseq = TSVseq.updated(0,triples.last.getSubject.getURI)

    val bindedSubject = predicates.head.head.concat(bindedPre)
    var tarqlBindPart = Seq[String](s"BIND(URI(?${predicates.head.head}) AS ?$bindedSubject)")
    var tarqlConstructPart = Seq.empty[String]//(s"?$tarqlSubject$bindedPre a ?${allPredicates(0)}$bindedPre)")

    triples.foreach(triple => {
      var alreadyIncluded = false
      var tripleObject = ""

      val triplePredicate = getSplitPredicate(triple.getPredicate.getURI)._2

      println(s"triplepre: $triplePredicate")
      if (predicates.exists(vector => vector.contains(triplePredicate))) {
        alreadyIncluded = true

        if (triple.getObject.isLiteral) {
          val datatype = getSplitPredicate(triple.getObject.getLiteralDatatype.getURI)._2
          tarqlBindPart = tarqlBindPart :+ s"BIND(xsd:$datatype(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"
          tarqlConstructPart = tarqlConstructPart :+ buildTarqlConstructStr(predicates ,triplePredicate, bindedSubject, bindedPre)
          tripleObject = triple.getObject.getLiteralLexicalForm
        }
        else if (triple.getObject.isURI) {
          tarqlConstructPart = tarqlConstructPart :+ buildTarqlConstructStr(predicates ,triplePredicate, bindedSubject, bindedPre)
          tarqlBindPart = tarqlBindPart :+ s"BIND(URI(?$triplePredicate) AS ?${triplePredicate}${bindedPre})"
          tripleObject = triple.getObject.getURI
        }
        else tripleObject = triple.getObject.getBlankNodeLabel

      }


      val index = predicates.indexOf(predicates.find(vector => vector.contains(triplePredicate)).get)

      if (alreadyIncluded) {
        TSVseq = TSVseq.updated(index, tripleObject)
      }
      else {
        TSVseq = TSVseq.updated(index, "")
      }
    })

    (Row.fromSeq(TSVseq),Row.fromSeq(tarqlConstructPart),Row.fromSeq(tarqlBindPart))
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

  def createTarqlMapFile(tarqlPrefixes: Seq[String], tarqlConstruct: Seq[String], tarqlBindings: Seq[String], tsvFile:File):File ={

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

//    FileUtil.unionFilesWithHeaderFile(headerTempDir, tempDir, targetFile)

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

      if (alreadyIncluded) {
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



