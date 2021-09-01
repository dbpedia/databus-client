package format

import better.files.File
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.mutable.ListBuffer

class FormatRoundTripTests extends FlatSpec {

  val spark: SparkSession = Spark.session
  val sparkContext: SparkContext = Spark.context

  val testFileDir: File = File("./src/test/resources/conversion")
  val outDir: File = testFileDir / "output"
  val tempDir: File = outDir / "tempDir"

  outDir.createDirectoryIfNotExists().clear()

  "roundtriptest" should "succeed for all RDF formats" in {

    println("Test Files:")
    val rdfFiles = (testFileDir / "rdfTriples").listRecursively
    val errorList: ListBuffer[String] = ListBuffer()

    while (rdfFiles.hasNext) {
      val inputFile = rdfFiles.next()
      println(inputFile.pathAsString)

      //read and write process
      val format = FileUtil.getFormatType(inputFile,"")
      val triples = new TripleHandler().read(inputFile.pathAsString, format)
      val outputFile =  new TripleHandler()
        .write(triples, format)
        .moveTo(outDir / inputFile.name)

      //read in input and output
      val statementsInput = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements().toList
      val statementsOutput = RDFDataMgr.loadModel(outputFile.pathAsString).listStatements().toList

      //compare both
      if (!statementsInput.containsAll(statementsOutput) || !statementsOutput.containsAll(statementsInput)) errorList += inputFile.pathAsString
    }

    //Result
    val success = {
      if (errorList.isEmpty) true
      else {
        println("ERRORS:")
        errorList.foreach(file => {
          println(s"$file did not convert properly")
        })
        false
      }
    }

    success shouldBe true
  }

  "roundtriptest" should "succeed for all RDF Quad formats" in {

    println("Test Files:")
    val quadFiles = (testFileDir / "rdfQuads").listRecursively

    val quadsHandler = new QuadsHandler()
    val errorList: ListBuffer[String] = ListBuffer()

    while (quadFiles.hasNext) {
      val inputFile = quadFiles.next()
      println(inputFile.pathAsString)

      //read in and write out to tsd file
      val format = FileUtil.getFormatType(inputFile,"")
      val quads = quadsHandler.read(inputFile.pathAsString, format)
      val outputFile = quadsHandler.write(quads, format).moveTo(outDir / inputFile.name)

      //read in input and output
      val statementsInput = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements().toList
      val statementsOutput = RDFDataMgr.loadModel(outputFile.pathAsString).listStatements().toList

      //compare both
      if (!statementsInput.containsAll(statementsOutput) || !statementsOutput.containsAll(statementsInput)) errorList += inputFile.pathAsString
    }

    //Result
    val success = {
      if (errorList.isEmpty) true
      else {
        println("ERRORS:")
        errorList.foreach(file => {
          println(s"$file did not convert properly")
        })
        false
      }
    }

    success shouldBe true
  }

  "roundtriptest" should "succeed for all TSD formats" in {
    println("Test Files:")
    val tsdFiles = (testFileDir / "tsd").listRecursively

    val errorList: ListBuffer[String] = ListBuffer()

    val tsdHandler = new TSDHandler()

    while (tsdFiles.hasNext) {
      val inputFile = tsdFiles.next()
      println(inputFile.pathAsString)

      //read in and write out to tsd file
      val format = FileUtil.getFormatType(inputFile,"")
      val dataFrame = tsdHandler.read(inputFile.pathAsString, format)
      val outputFile = tsdHandler.write(dataFrame, format).moveTo(outDir / inputFile.name)

      //read in input and output data
      val dataInput = tsdHandler.read(inputFile.pathAsString, format).sort()
      val dataOutput = tsdHandler.read(outputFile.pathAsString, format).sort()

      //compare both
      if (!checkDFEquality(dataInput,dataOutput) || !checkDFEquality(dataOutput,dataInput)) errorList += inputFile.pathAsString
    }

    //Result
    val success = {
      if (errorList.isEmpty) true
      else {
        println("ERRORS:")
        errorList.foreach(file => {
          println(s"$file did not convert properly")
        })
        false
      }
    }

    success shouldBe true
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
}


//  def downloadFiles(testFileDir:File): File ={
//
//    val queryTestFiles=
//      """
//        |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
//        |PREFIX dataid-cv: <http://dataid.dbpedia.org/ns/cv#>
//        |PREFIX dct: <http://purl.org/dc/terms/>
//        |PREFIX dcat:  <http://www.w3.org/ns/dcat#>
//        |
//        |# Get all files
//        |SELECT DISTINCT ?file WHERE {
//        | 	?dataset dataid:artifact <https://databus.dbpedia.org/fabian/databus-client-testbed/format-testbed> .
//        |	?dataset dcat:distribution ?distribution .
//        |	{
//        |		?distribution dct:hasVersion ?latestVersion
//        |		{
//        |			SELECT (?version as ?latestVersion) WHERE {
//        |				?dataset dataid:artifact <https://databus.dbpedia.org/fabian/databus-client-testbed/format-testbed> .
//        |				?dataset dct:hasVersion ?version .
//        |			} ORDER BY DESC (?version) LIMIT 1
//        |		}
//        |	}
//        |	?distribution dcat:downloadURL ?file .
//        |}
//        |""".stripMargin
//
//    Downloader.downloadWithQuery(queryTestFiles, testFileDir)
//
//    testFileDir
//  }