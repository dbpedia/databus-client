package conversionTests.mapping

import java.net.URL

import better.files.File
import org.apache.jena.Jena
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory, Statement}
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.filehandling.{FileHandler, FileUtil}
import org.dbpedia.databus.filehandling.convert.format.csv.CSVHandler
import org.dbpedia.databus.filehandling.convert.format.rdf.RDFHandler
import org.dbpedia.databus.filehandling.download.Downloader
import org.scalatest.{FlatSpec, Tag}

import scala.collection.mutable

class roundTripTests extends FlatSpec{

  val spark: SparkSession = SparkSession.builder()
    .appName(s"Triple reader")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("WARN")

  val testFileDir = File("./src/test/resources/testFiles/roundTripTestFiles/mapping/")
  val tempDir = File("./src/test/resources/testFiles/roundTripTestFiles/mapping/tempDir")


  "roundtriptest" should "succeed for RDF to TSV and back to RDF" in{

    val inputFile = testFileDir/"ntriples.nt"
    val outputFile = testFileDir / s"${inputFile.nameWithoutExtension(true)}_out${inputFile.extension.get}"

    val downloadURL= new URL("http://dbpedia-mappings.tib.eu/databus-repo/eisenbahnplatte/databus-client-testbed/format-testbed/2019.08.30/format-testbed_bob2.nt")
    val delimiter = ";"
    val quotation = "null"
    val tsdFormat = "tsv"

    Downloader.downloadUrlToFile(downloadURL, inputFile, createParentDirectory = true)

    TSDtoRDF(
      writeRDFtoTSD(inputFile, delimiter, tsdFormat),
      outputFile,
      tsdFormat,
      spark,
      delimiter,
      quotation
    )

    assert(RDFEqualityWithMissingTriples(inputFile,outputFile, tsdFormat))
  }


  def writeRDFtoTSD(inputFile:File, delimiter:String, outputFormat:String):File={
    val inputFormat = FileHandler.getFormatType(inputFile, "")

    tempDir.delete(swallowIOExceptions = true)
    val triples = RDFHandler.readRDF(inputFile, inputFormat, spark: SparkSession)
    val mappingFile = CSVHandler.writeTriples(tempDir, triples, outputFormat, delimiter.toCharArray.head,spark)

    val csvFile = testFileDir/s"ntriples.$outputFormat"
    try {
      FileUtil.unionFiles(tempDir, csvFile)
      if (mappingFile.exists && mappingFile != File("")) {
        val mapDir = testFileDir/"mappings"
        mapDir.createDirectoryIfNotExists()
        mappingFile.moveTo(mapDir / FileUtil.getSha256(csvFile), overwrite = true)
      }
    }
    catch {
      case _: RuntimeException => println(s"File $csvFile already exists") //deleteAndRestart(inputFile, inputFormat, outputFormat, targetFile: File)
    }

    tempDir.delete()
    csvFile
  }

  def TSDtoRDF(csvFile:File, outputFile:File, tsdFormat:String, spark:SparkSession,delimiter:String,quotation:String):File={
    val csvtriples = CSVHandler.readAsTriples(csvFile, tsdFormat, spark, Seq((testFileDir/"mappings" / FileUtil.getSha256(csvFile)).pathAsString,delimiter,quotation))

    RDFHandler.writeRDF(tempDir,csvtriples,"nt", spark)

    try {
      FileUtil.unionFiles(tempDir, outputFile)
      tempDir.delete()
    }
    catch {
      case _: RuntimeException => println("File $targetFile already exists")
    }
    outputFile
  }

  def RDFEqualityWithMissingTriples(inputFile:File,outputFile:File, tsdFormat:String):Boolean={
    val inputModel = RDFDataMgr.loadModel(inputFile.pathAsString)

    val statements= inputModel.listStatements()

    val addModel = ModelFactory.createDefaultModel()
    val removeModel = ModelFactory.createDefaultModel()

    while (statements.hasNext) {
      val stmt = statements.nextStatement()
      if (stmt.getObject.isLiteral) {
        if (!stmt.getObject.asLiteral().getLanguage.isEmpty) {
          removeModel.add(stmt)
          val newStmt = ResourceFactory.createStatement(
            ResourceFactory.createResource(stmt.getSubject.getURI),
            ResourceFactory.createProperty(stmt.getPredicate.getURI),
            ResourceFactory.createPlainLiteral(stmt.getObject.asLiteral().getString)
          )
          addModel.add(newStmt)
        }
      }

    }


    println("Statements that can't be mapped:")
    val stsms2 =removeModel.listStatements()
    while (stsms2.hasNext) println(stsms2.nextStatement())

    println("Above statements are changed for testing:")
    val stsms =addModel.listStatements()
    while (stsms.hasNext) println(stsms.nextStatement())
    //
    inputModel.remove(removeModel)
    inputModel.add(addModel)

    val statementsInput = inputModel.listStatements().toList
    val outputModel = RDFDataMgr.loadModel(outputFile.pathAsString)
    val statementsOutput = outputModel.listStatements().toList


    println(s"\nIs all output data in the input data? ${statementsInput.containsAll(statementsOutput)}")
    println(s"Is all input data in the output data? ${statementsOutput.containsAll(statementsInput)}\n")


    import collection.JavaConverters._

    val outputContainsAllofInput = {
      if (tsdFormat == "tsv") equalityIfTSV(inputModel, outputModel)
      else {
        if (statementsOutput.containsAll(statementsInput)) true
        else {
          showUnEquality(statementsOutput.asScala, statementsInput.asScala)
          false
        }
      }
    }

    val inputContainsAllOutputData =  {
      if (statementsInput.containsAll(statementsOutput)) true
      else {
        showUnEquality(statementsInput.asScala,statementsOutput.asScala)
        false
      }
    }

    if (inputContainsAllOutputData && outputContainsAllofInput) true
    else false
  }

  def showUnEquality(it1:mutable.Buffer[Statement], it2: mutable.Buffer[Statement]):Unit ={
    var succeed=true
    it2.foreach(stmt => {
      if(!it1.contains(stmt)) {
        println(s"STATEMENT: $stmt")
        succeed=false
      }
    })
  }

  def equalityIfTSV(inputModel:Model, outputModel:Model ) :Boolean={
    var equal = true

    inputModel.remove(outputModel)

    val stmtsOnlyInInput = inputModel.listStatements()

    println("Some data can be lost in TSV because only one object can be saved per predicate. The following instructions are not in the outputModel:")

    while (stmtsOnlyInInput.hasNext) {
      val stmt = stmtsOnlyInInput.nextStatement()
      val prop = outputModel.getProperty(stmt.getSubject, stmt.getPredicate)

      println(s"Statement: $prop")
      if (prop == null) {equal=false}
    }


    equal
  }
}
