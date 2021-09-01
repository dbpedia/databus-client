package format

import better.files.File
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory, Statement}
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.CompileConfig
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.mapping.{RDF_Quads_Mapper, RDF_Triples_Mapper, TSD_Mapper}
import org.scalatest.FlatSpec

import scala.collection.mutable

class MappingRoundTripTests extends FlatSpec {

  val tempDir: File = File("./target/databus.tmp/temp/")
  tempDir.createDirectoryIfNotExists()

  val spark: SparkSession = Spark.session
  val sparkContext: SparkContext = Spark.context

  val testFileDir: File = File("./src/test/resources/mapping")
  val testFileTempDir: File = testFileDir / "temp"
  testFileTempDir.createDirectoryIfNotExists().clear()

  "roundtriptest" should "succeed for RDFTriples <-> RDFQuads" in {

    val inputFile = testFileDir / "test.nt"

    val tripleHandler = new TripleHandler()
    val quadsHandler = new QuadsHandler()

    val triples = tripleHandler.read(inputFile.pathAsString, "nt")
    val quads = RDF_Triples_Mapper.map_to_quads(triples, "")

    val quadFile = quadsHandler.write(quads, "nq")
    val quads2 = quadsHandler.read(quadFile.pathAsString, "nq")

    val triples2 = RDF_Quads_Mapper.map_to_triples(quads2)
    val outputFile = tripleHandler.write(triples2.head.graph, "nt")

    //read in input and output
    val statementsInput = RDFDataMgr.loadModel(inputFile.pathAsString).listStatements().toList
    val statementsOutput = RDFDataMgr.loadModel(outputFile.pathAsString).listStatements().toList

    //compare both
    assert(statementsInput.containsAll(statementsOutput) && statementsOutput.containsAll(statementsInput))
  }

  "roundtriptest" should "succeed for RDFTriples <-> TSD" in {

    val inputFile = testFileDir / "test.nt"

    val config: CompileConfig = new CompileConfig(
      inputFormat = "tsv",
      inputCompression = "",
      outputFormat = "",
      outputCompression = "",
      target = File(""),
      mapping = (testFileTempDir / "mappingFile.sparql").pathAsString,
      delimiter = '\t',
      quotation = '"',
      createMapping = true,
      graphURI = "",
      outFile = File("")
    )

    val tripleHandler = new TripleHandler()
    val tsdHandler = new TSDHandler()
    val triples = tripleHandler.read(inputFile.pathAsString, "nt")
    val dataFrame = RDF_Triples_Mapper.map_to_tsd(triples, createMapping = true)

    File("./target/databus.tmp/mappingFile.sparql").moveTo(File(config.mapping))

    val tsvFile = tsdHandler.write(dataFrame, "tsv")
    val triples2 = TSD_Mapper.map_to_triples(tsvFile, config)
    val outFile = tripleHandler.write(triples2, "nt")

    assert(RDFEqualityWithMissingTriples(inputFile, outFile, "tsv"))
  }


  def RDFEqualityWithMissingTriples(inputFile: File, outputFile: File, tsdFormat: String): Boolean = {
    val inputModel = RDFDataMgr.loadModel(inputFile.pathAsString)

    val statements = inputModel.listStatements()

    val addModel = ModelFactory.createDefaultModel()
    val removeModel = ModelFactory.createDefaultModel()

    while (statements.hasNext) {
      val stmt = statements.nextStatement()
      if (stmt.getObject.isLiteral) {
        if (stmt.getObject.asLiteral().getLanguage.nonEmpty) {
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


    //    println("Statements that can't be mapped:")
    //    val stsms2 =removeModel.listStatements()
    //    while (stsms2.hasNext) println(stsms2.nextStatement())

    inputModel.remove(removeModel)
    inputModel.add(addModel)

    val statementsInput = inputModel.listStatements().toList
    val outputModel = RDFDataMgr.loadModel(outputFile.pathAsString)
    val statementsOutput = outputModel.listStatements().toList

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

    val inputContainsAllOutputData = {
      if (statementsInput.containsAll(statementsOutput)) true
      else {
        showUnEquality(statementsInput.asScala, statementsOutput.asScala)
        false
      }
    }

    if (inputContainsAllOutputData && outputContainsAllofInput) true
    else false
  }

  def showUnEquality(it1: mutable.Buffer[Statement], it2: mutable.Buffer[Statement]): Unit = {
    var succeed = true
    it2.foreach(stmt => {
      if (!it1.contains(stmt)) {
        //        println(s"STATEMENT: $stmt")
        succeed = false
      }
    })
  }

  def equalityIfTSV(inputModel: Model, outputModel: Model): Boolean = {
    var equal = true

    inputModel.remove(outputModel)

    val stmtsOnlyInInput = inputModel.listStatements()

    while (stmtsOnlyInInput.hasNext) {
      val stmt = stmtsOnlyInInput.nextStatement()
      val prop = outputModel.getProperty(stmt.getSubject, stmt.getPredicate)

      //      println(s"Statement: $stmt")
      if (prop == null) {
        equal = false
      }
    }

    equal
  }
}
