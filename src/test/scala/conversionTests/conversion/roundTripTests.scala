package conversionTests.conversion

import better.files.File
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.download.Downloader
import org.dbpedia.databus.client.filehandling.{FileHandler, FileUtil}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.mutable.ListBuffer
class roundTripTests extends FlatSpec{

  val spark: SparkSession = SparkSession.builder()
    .appName(s"Triple reader")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  implicit val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("WARN")


  val testFileDir:File = downloadFiles(File("./src/test/resources/roundTripTestFiles/conversion"))
//  val testFileDir = File("./src/test/resources/roundTripTestFiles")
  val outDir:File = testFileDir / "output"
  val tempDir:File = outDir / "tempDir"

  def downloadFiles(testFileDir:File): File ={

    val queryTestFiles=
      """
        |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
        |PREFIX dataid-cv: <http://dataid.dbpedia.org/ns/cv#>
        |PREFIX dct: <http://purl.org/dc/terms/>
        |PREFIX dcat:  <http://www.w3.org/ns/dcat#>
        |
        |# Get all files
        |SELECT DISTINCT ?file WHERE {
        | 	?dataset dataid:artifact <https://databus.dbpedia.org/fabian/databus-client-testbed/format-testbed> .
        |	?dataset dcat:distribution ?distribution .
        |	{
        |		?distribution dct:hasVersion ?latestVersion
        |		{
        |			SELECT (?version as ?latestVersion) WHERE {
        |				?dataset dataid:artifact <https://databus.dbpedia.org/fabian/databus-client-testbed/format-testbed> .
        |				?dataset dct:hasVersion ?version .
        |			} ORDER BY DESC (?version) LIMIT 1
        |		}
        |	}
        |	?distribution dcat:downloadURL ?file .
        |}
        |""".stripMargin

    Downloader.downloadWithQuery(queryTestFiles, testFileDir)

    testFileDir
  }

  def readAndWriteTriples(inputFile:File, tempDir:File, spark:SparkSession): File = {
    val format = FileHandler.getFormatType(inputFile,"")
    val triples = TripleHandler.read(inputFile.pathAsString, format)

    if(format=="rdf") TripleHandler.write(triples, "rdfxml")
    else TripleHandler.write(triples, format)

    val targetFile = tempDir.parent / inputFile.name

    try {
      FileUtil.unionFiles(tempDir, targetFile)
      tempDir.delete()
    }
    catch {
      case _: RuntimeException => "File $targetFile already exists"
    }

    targetFile
  }

  def readAndWriteTSD(inputFile:File, tempDir:File, spark:SparkSession):File={
    val format = FileHandler.getFormatType(inputFile,"")
    val delimiter = {
      if(format=="csv") ','
      else '\t'
    }
    val dataFrame = TSDHandler.read(inputFile.pathAsString, format, delimiter)

    TSDHandler.write(dataFrame, format, delimiter)

    val targetFile = tempDir.parent / inputFile.name

    try {
      FileUtil.unionFiles(tempDir, targetFile)
      tempDir.delete()
    }
    catch {
      case _: RuntimeException => "File $targetFile already exists"
    }

    targetFile
  }

  def readTSDasDF(tsdFile:File):DataFrame ={
    val format = FileHandler.getFormatType(tsdFile,"")
    val delimiter = {
      if (format == "csv") ","
      else "\t"
    }

    spark.read.format("csv")
      .option("sep", delimiter.toString)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(tsdFile.pathAsString)

  }

  def checkDFEquality(df1:DataFrame, df2:DataFrame):Boolean ={
    if (df1.columns.deep == df2.columns.deep) {
      val rowsExist:ListBuffer[Boolean] = ListBuffer()
      val array1 = df1.collect()
      val array2 = df2.collect()

      array1.foreach(
        row => {
//          println(row)
          var rowExists = false
          array2.foreach(
            rowArray2 => {
              if(row.equals(rowArray2)) rowExists=true
            }
          )
          rowsExist += rowExists
        }
      )
      if (rowsExist.contains(false)) false
      else true
    }
    else false
  }

  "roundtriptest" should "succeed for all RDF formats" in {


    val rdfFiles = testFileDir
      .listRecursively
      .filter(file =>
        !(file.nameWithoutExtension(true) matches "dataid")
      )
      .filter(file =>
        file.extension().getOrElse("") matches ".rdf|.nt|.ttl|.jsonld"
      )

    val successList: ListBuffer[Seq[String]] = ListBuffer()

    while (rdfFiles.hasNext) {
      val inputFile = rdfFiles.next()
      val outputFile = readAndWriteTriples(inputFile, tempDir, spark)

      println(inputFile.pathAsString)

      val statementsInput = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements().toList
      val statementsOutput = RDFDataMgr.loadModel(outputFile.pathAsString).listStatements().toList

      if (statementsInput.containsAll(statementsOutput) && statementsOutput.containsAll(statementsInput)) successList += Seq(inputFile.pathAsString, "succeed")
      else successList += Seq(inputFile.pathAsString, "error")
    }


    println(successList.isEmpty)
    var success = {
      if (successList.isEmpty) false
      else true
    }

    successList.foreach(conversion => {
      if (conversion(1) == "error") {
        success = false
        println(s"${conversion.head} did not convert properly")
      }
    })

    success shouldBe true
  }

  "roundtriptest" should "succeed for all TSD formats" in {

    val tsdFiles = testFileDir
      .listRecursively
      .filter(file =>
        !(file.nameWithoutExtension(true) matches "dataid")
      )
      .filter(file =>
        file.extension().getOrElse("") matches ".tsv|.csv"
      )

    val successList: ListBuffer[Seq[String]] = ListBuffer()

    while (tsdFiles.hasNext) {
      val inputFile = tsdFiles.next()
      val outputFile = readAndWriteTSD(inputFile, tempDir, spark)

      println(inputFile.pathAsString)


      val dataInput = readTSDasDF(inputFile).sort()
      val dataOutput = readTSDasDF(outputFile).sort()

//      dataInput.show()
//      dataOutput.show()

      if (checkDFEquality(dataInput,dataOutput) && checkDFEquality(dataOutput,dataInput)) successList += Seq(inputFile.pathAsString, "succeed")
      else successList += Seq(inputFile.pathAsString, "error")
    }

    var success = {
      if (successList.isEmpty) false
      else true
    }

    successList.foreach(conversion => {
      println(s"${conversion.head},${conversion(1)}")
      if (conversion(1) == "error") {
        success = false
        println(s"${conversion.head} did not convert properly")
      }
    })

    success shouldBe true
  }

  "roundtriptest" should "succeed for all RDF Quad formats" in {
    val quads = QuadsHandler.read("src/test/resources/roundTripTestFiles/conversion/quads/trig.trig","trig")
    QuadsHandler.write(quads, "trig")

    val targetFile = tempDir.parent / "nq.nq"

    try {
      FileUtil.unionFiles(tempDir, targetFile)
      tempDir.delete()
    }
    catch {
      case _: RuntimeException => "File $targetFile already exists"
    }

    targetFile
  }
}
