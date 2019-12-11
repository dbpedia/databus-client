package org.dbpedia.databus.filehandling.convert.format

import java.io._

import better.files.File
import org.apache.commons.compress.archivers.dump.InvalidFormatException
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.apache.commons.io.FileUtils
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.api.Databus.Format.Value
import org.dbpedia.databus.filehandling.{SourceHandler, FileUtil}
import org.dbpedia.databus.filehandling.FileUtil.copyStream
import org.dbpedia.databus.filehandling.convert.format.csv.Writer
import org.dbpedia.databus.filehandling.convert.format.rdf.read.{JSONL_Reader, NTriple_Reader, RDF_Reader, TTL_Reader}
import org.dbpedia.databus.filehandling.convert.format.rdf.write._
import org.dbpedia.databus.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.control.Breaks.{break, breakable}

object Converter {

  def convertFormat(inputFile: File, inputFormat: String, outputFormat: String): File = {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val tempDir = File("./target/databus.tmp/temp/")
    if (tempDir.exists) tempDir.delete()
    val mappingFile = tempDir / "mappingFile"
    val targetFile: File = tempDir / inputFile.nameWithoutExtension.concat(s".$outputFormat")


    if (EquivalenceClasses.CSVTypes.contains(outputFormat) && EquivalenceClasses.CSVTypes.contains(inputFormat)) {
      val data: DataFrame = readNonTripleData(inputFile, inputFormat, spark: SparkSession)
      writeNonTripleData(tempDir, data, outputFormat, spark)
    }
    else {
      val triples: RDD[Triple] = readTripleData(inputFile, inputFormat, spark: SparkSession)
      writeTripleData(tempDir, mappingFile, triples, outputFormat, spark)
    }

    try {
      FileUtil.unionFiles(tempDir, targetFile)
    }
    catch {
      case _: RuntimeException => LoggerFactory.getLogger("UnionFilesLogger").error(s"File $targetFile already exists") //deleteAndRestart(inputFile, inputFormat, outputFormat, targetFile: File)
    }

    if (mappingFile.exists) mappingFile.moveTo(File("./mappings/") / FileUtil.getSha256(targetFile), overwrite = true)

    targetFile
  }

  def readNonTripleData(inputFile: File, inputFormat: String, spark: SparkSession): DataFrame = {

    inputFormat match {
      case "tsv" =>
        csv.Reader.csv_to_df(inputFile.pathAsString, '\t', spark)
      case "csv" =>
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).asInstanceOf[Character]
        csv.Reader.csv_to_df(inputFile.pathAsString, delimiter, spark)
    }

  }

  def writeNonTripleData(tempDir:File, data:DataFrame, outputFormat: String, spark: SparkSession): Unit = {

    outputFormat match {
      case "tsv" =>
        data.coalesce(1).write
          .option("delimiter", "\t")
          .option("emptyValue", "")
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "false")
          .csv(tempDir.pathAsString)
      case "csv" =>
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).asInstanceOf[Character]
        data.coalesce(1).write
          .option("delimiter", delimiter.toString)
          .option("emptyValue", "")
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "false")
          .csv(tempDir.pathAsString)
    }
  }

  def readTripleData(inputFile: File, inputFormat: String, spark: SparkSession): RDD[Triple] = {

    inputFormat match {
      case "nt" =>
        NTriple_Reader.read(spark, inputFile)

      case "rdf" =>
        RDF_Reader.read(spark, inputFile)

      case "ttl" =>
        //wie geht das besser?
        try {
          val data = NTriple_Reader.read(spark, inputFile)
          data.isEmpty()
          data
        }
        catch {
          case _: org.apache.spark.SparkException => TTL_Reader.read(spark, inputFile)
        }

      case "jsonld" =>
        RDF_Reader.read(spark, inputFile) //Ein Objekt pro Datei

      case "tsv" =>
        val mappingFile = scala.io.StdIn.readLine("Please type Path to Mapping File:\n")
        csv.Reader.csv_to_rddTriple(mappingFile, inputFile.pathAsString, '\t', sc = spark.sparkContext)

      case "csv" =>
        val mappingFile:String = scala.io.StdIn.readLine("Please type Path to Mapping File:\n")
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).asInstanceOf[Character]
        val quotation = scala.io.StdIn.readLine("Please type quote Charater of CSV file:\n(e.x. ' \" ' for double quoted entries or ' null ' if there's no quotation)\n")
        val quoteChar = quotation match {
          case "null" => null
          case _ => quotation.toCharArray.apply(0).asInstanceOf[Character]
        }
        csv.Reader.csv_to_rddTriple(mappingFile, inputFile.pathAsString, delimiter, quoteChar, spark.sparkContext)

//      case "jsonl" =>
//        try { //Mehrere Objekte pro Datei
//          JSONL_Reader.readJSONL(spark, inputFile)
//        } catch {
//          case _: SparkException =>
//            println("Json Object ueber mehrere Zeilen")
//            RDF_Reader.read(spark, inputFile)
//        }
    }
  }

  def writeTripleData(tempDir: File, mappingFile: File, data: RDD[Triple], outputFormat: String, spark: SparkSession): Unit = {


    outputFormat match {
      case "nt" =>
        NTriple_Writer.convertToNTriple(data).saveAsTextFile(tempDir.pathAsString)

      case "tsv" =>
        manageTripleToTSV(data, "\t", tempDir, mappingFile, spark)
      case "csv" =>
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).asInstanceOf[Character]
        manageTripleToTSV(data, delimiter.toString, tempDir, mappingFile, spark)
      case "ttl" =>
        TTL_Writer.convertToTTL(data, spark).coalesce(1).saveAsTextFile(tempDir.pathAsString)

      case "jsonld" =>
        JSONLD_Writer.convertToJSONLD(data, spark).saveAsTextFile(tempDir.pathAsString)

      case "rdfxml" =>
        RDF_Writer.convertToRDF(data, spark, RDFFormat.RDFXML).coalesce(1).saveAsTextFile(tempDir.pathAsString)
    }

  }

  def manageTripleToTSV(data:RDD[Triple], delimiter:String, tempDir:File, mappingFile:File, spark: SparkSession): Unit ={
    val createMappingFile = scala.io.StdIn.readLine("Type 'y' or 'yes' if you want to create a MappingFile.\n")

    if (createMappingFile.matches("yes|y")) {
      File("./mappings/").createDirectoryIfNotExists()

      val tsvData = Writer.convertToTSV(data, spark, createMappingFile = true)
      tsvData._1.coalesce(1).write
        .option("delimiter", delimiter)
        .option("emptyValue", "")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "false")
        .csv(tempDir.pathAsString)

      Writer.createTarqlMapFile(tsvData._2, mappingFile)
    }

    else {
      val tsvData = Writer.convertToTSV(data, spark)
      tsvData.coalesce(1).write
        .option("delimiter", delimiter)
        .option("emptyValue", "")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "false")
        .csv(tempDir.pathAsString)
    }
  }

}
