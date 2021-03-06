package org.dbpedia.databus.client.filehandling.convert.format

import better.files.File
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.format.csv.CSVHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.RDFHandler
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}

/**
 * Converter for tsv, csv and several RDF serializations (nt,ttl,rdfxml,json-ld)
 */
object Converter {

  var delimiter = ""
  var createMappingFile:Option[Boolean] = None

  /**
   * converts a file to a desired format
   *
   * @param inputFile input file
   * @param inputFormat input format
   * @param outputFormat output format
   * @param sha sha256-sum of input file
   * @return converted file
   */
  def convertFormat(inputFile: File, inputFormat: String, outputFormat: String, sha:String): File = {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val tempDir = File("./target/databus.tmp/temp/")
    if (tempDir.exists) tempDir.delete()

    val targetFile: File = tempDir / inputFile.nameWithoutExtension.concat(s".$outputFormat")
    var mappingFile = File("")

    if (EquivalenceClasses.RDFTypes.contains(outputFormat)){

      val triples = {
        if (EquivalenceClasses.RDFTypes.contains(inputFormat)) {
          RDFHandler.readRDF(inputFile, inputFormat, spark: SparkSession)

        }
        else{ // if (EquivalenceClasses.CSVTypes.contains(inputFormat)){
          val possibleMappings = QueryHandler.getPossibleMappings(sha)
          var triples = sparkContext.emptyRDD[org.apache.jena.graph.Triple]

          breakable {
            possibleMappings.foreach(mapping => {
              val mappingFileAndInfo = QueryHandler.getMappingFileAndInfo(mapping)
              triples = CSVHandler.readAsTriples(inputFile, inputFormat, spark: SparkSession, mappingFileAndInfo)
              if (!triples.isEmpty()) break
            })
          }

          triples
        }
      }

      RDFHandler.writeRDF(tempDir, triples, outputFormat, spark)
    }

    else if (EquivalenceClasses.CSVTypes.contains(outputFormat)) {
      if (EquivalenceClasses.CSVTypes.contains(inputFormat)) {
        val data: DataFrame = CSVHandler.read(inputFile, inputFormat, spark: SparkSession)
        CSVHandler.write(tempDir, data, outputFormat, spark)
      }
      else {
        val triples = RDFHandler.readRDF(inputFile, inputFormat, spark: SparkSession)

        if (createMappingFile.isEmpty){
          createMappingFile = {
            if (scala.io.StdIn.readLine("Type 'y' or 'yes' if you want to create a mapping file.\n") matches "yes|y") Option(true)
            else Option(false)
          }
        }

        if (delimiter.isEmpty){
          delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).toString
        }

        mappingFile = CSVHandler.writeTriples(tempDir, triples, outputFormat, delimiter.charAt(0), spark, createMappingFile.get)
      }
    }

    try {
      FileUtil.unionFiles(tempDir, targetFile)
      if (mappingFile.exists && mappingFile != File("")) {
        val mapDir = File("./mappings/")
          mapDir.createDirectoryIfNotExists()
        mappingFile.moveTo(mapDir / FileUtil.getSha256(targetFile), overwrite = true)
      }
    }
    catch {
      case _: RuntimeException => LoggerFactory.getLogger("UnionFilesLogger").error(s"File $targetFile already exists") //deleteAndRestart(inputFile, inputFormat, outputFormat, targetFile: File)
    }

    targetFile
  }

}
