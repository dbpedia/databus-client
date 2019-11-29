package org.dbpedia.databus.filehandling.converter

import java.io._

import better.files.File
import org.apache.commons.compress.archivers.dump.InvalidFormatException
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.apache.commons.io.FileUtils
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFFormat
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.filehandling.FileUtil.copyStream
import org.dbpedia.databus.filehandling.converter.mappings.TSV_Writer
import org.dbpedia.databus.filehandling.converter.rdf_reader.{JSONL_Reader, NTriple_Reader, RDF_Reader, TTL_Reader}
import org.dbpedia.databus.filehandling.converter.rdf_writer._
import org.dbpedia.databus.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.control.Breaks.{break, breakable}

object Converter {

  def convertFile(inputFile: File, dest_dir: File, outputFormat: String, outputCompression: String): Unit = {
    println(s"input file:\t\t${inputFile.pathAsString}")
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(inputFile.toJava))

    val compressionInputFile = getCompressionType(bufferedInputStream)
    val formatInputFile = getFormatType(inputFile, compressionInputFile)

    if ((outputCompression == compressionInputFile || outputCompression == "same") && (outputFormat == formatInputFile || outputFormat == "same")) {
      val outputStream = new FileOutputStream(getOutputFile(inputFile, formatInputFile, compressionInputFile, dest_dir).toJava)
      copyStream(new FileInputStream(inputFile.toJava), outputStream)
    }
    else if (outputCompression != compressionInputFile && (outputFormat == formatInputFile || outputFormat == "same")) {
      val decompressedInStream = decompress(bufferedInputStream)
      val compressedFile = getOutputFile(inputFile, formatInputFile, outputCompression, dest_dir)
      val compressedOutStream = compress(outputCompression, compressedFile)
      copyStream(decompressedInStream, compressedOutStream)
    }

    //  With FILEFORMAT CONVERSION
    else {

      if (!isSupportedInFormat(formatInputFile)) return

      var newOutCompression = outputCompression
      if (outputCompression == "same") newOutCompression = compressionInputFile

      val targetFile = getOutputFile(inputFile, outputFormat, newOutCompression, dest_dir)
      var typeConvertedFile = File("")

      if (!(compressionInputFile == "")) {
        val decompressedInStream = decompress(bufferedInputStream)
        val decompressedFile = File("./target/databus.tmp/") / inputFile.nameWithoutExtension(true).concat(s".$formatInputFile")
        copyStream(decompressedInStream, new FileOutputStream(decompressedFile.toJava))
        typeConvertedFile = convertFormat(decompressedFile, formatInputFile, outputFormat)

        decompressedFile.delete()
      }
      else {
        typeConvertedFile = convertFormat(inputFile, formatInputFile, outputFormat)
      }

      val compressedOutStream = compress(newOutCompression, targetFile)
      copyStream(new FileInputStream(typeConvertedFile.toJava), compressedOutStream)

      //DELETE TEMPDIR
      //      if (typeConvertedFile.parent.exists) typeConvertedFile.parent.delete()

    }

  }

  private[this] def isSupportedInFormat(format: String): Boolean = {
    if (format.matches("rdf|ttl|nt|jsonld|tsv|csv")) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Input file format $format is not supported.")
      println(s"Input file format $format is not supported.")
      false
    }
  }

  private[this] def getCompressionType(fileInputStream: BufferedInputStream): String = {
    try {
      var ctype = CompressorStreamFactory.detect(fileInputStream)
      if (ctype == "bzip2") {
        ctype = "bz2"
      }
      ctype
    }
    catch {
      case _: CompressorException => ""
      case _: ExceptionInInitializerError => ""
      case _: NoClassDefFoundError => ""
    }
  }

  private[this] def getFormatType(inputFile: File, compressionInputFile: String): String = {
    {
      try {
        if (!(getFormatTypeWithDataID(inputFile) == "")) {
          getFormatTypeWithDataID(inputFile)
        } else {
          getFormatTypeWithoutDataID(inputFile, compressionInputFile)
        }
      } catch {
        case _: FileNotFoundException => getFormatTypeWithoutDataID(inputFile, compressionInputFile)
      }
    }
  }

  private[this] def getFormatTypeWithDataID(inputFile: File): String = {
    // Suche in Dataid.ttl nach allen Zeilen die den Namen der Datei enthalten
    val source = Source.fromFile((inputFile.parent / "dataid.ttl").toJava, "UTF-8")
    val lines = source.getLines().filter(_ contains s"${inputFile.name}")

    val regex = s"<\\S*dataid.ttl#${inputFile.name}\\S*>".r
    var fileURL = ""

    for (line <- lines) {
      breakable {
        for (x <- regex.findAllMatchIn(line)) {
          fileURL = x.toString().replace(">", "").replace("<", "")
          break
        }
      }
    }

    source.close()
    QueryHandler.getTypeOfFile(fileURL, inputFile.parent / "dataid.ttl")
  }

  //SIZE DURCH LENGTH ERSETZEN
  private[this] def getFormatTypeWithoutDataID(inputFile: File, compression: String): String = {
    val split = inputFile.name.split("\\.")

    if (compression == "") split(split.size - 1)
    else split(split.size - 2)
  }

  //  private[this] def getOutputFile(inputFile: File, outputFormat: String, outputCompression: String, src_dir: File, dest_dir: File): File = {
  //
  //    val nameWithoutExtension = inputFile.nameWithoutExtension
  //    val name = inputFile.name
  //    var filepath_new = ""
  //    val dataIdFile = inputFile.parent / "dataid.ttl"
  //
  //    val newOutputFormat = {
  //      if (outputFormat == "rdfxml") "rdf"
  //      else outputFormat
  //    }
  //
  //    if (dataIdFile.exists) {
  //      val dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
  //      filepath_new = dest_dir.pathAsString.concat("/")
  //      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
  //      filepath_new = filepath_new.concat(nameWithoutExtension)
  //    }
  //    else {
  //      // changeExtensionTo() funktioniert nicht bei noch nicht existierendem File, deswegen ausweichen über Stringmanipulation
  //      //      filepath_new = inputFile.pathAsString.replace(src_dir.pathAsString, dest_dir.pathAsString.concat("/NoDataID"))
  //      filepath_new = dest_dir.pathAsString.concat("/NoDataID").concat(inputFile.pathAsString.replace(File(".").pathAsString, "")) //.concat(nameWithoutExtension)
  //
  //      filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
  //    }
  //
  //    if (outputCompression.isEmpty) {
  //      filepath_new = filepath_new.concat(".").concat(newOutputFormat)
  //    }
  //    else {
  //      filepath_new = filepath_new.concat(".").concat(newOutputFormat).concat(".").concat(outputCompression)
  //    }
  //
  //    val outputFile = File(filepath_new)
  //    //create necessary parent directories to write the outputfile there, later
  //    outputFile.parent.createDirectoryIfNotExists(createParents = true)
  //
  //    println(s"converted file:\t${outputFile.pathAsString}\n")
  //
  //    outputFile
  //  }

  private[this] def getOutputFile(inputFile: File, outputFormat: String, outputCompression: String, dest_dir: File): File = {

    val nameWithoutExtension = inputFile.nameWithoutExtension

    val dataIdFile = inputFile.parent / "dataid.ttl"

    val newOutputFormat = {
      if (outputFormat == "rdfxml") "rdf"
      else outputFormat
    }

    val outputDir = {
      if (dataIdFile.exists) QueryHandler.getTargetDir(dataIdFile, dest_dir)
      else
        File(dest_dir.pathAsString.concat("/NoDataID")
          .concat(inputFile.pathAsString.splitAt(inputFile.pathAsString.lastIndexOf("/"))._1
            .replace(File(".").pathAsString, "")
          )
        )
    }

    val newName = {
      if (outputCompression.isEmpty) s"$nameWithoutExtension.$newOutputFormat"
      else s"$nameWithoutExtension.$newOutputFormat.$outputCompression"
    }

    val outputFile = outputDir / newName

    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    println(s"output file:\t${outputFile.pathAsString}\n")

    outputFile
  }

  private[this] def decompress(bufferedInputStream: BufferedInputStream): InputStream = {
    //Welche Funktion hat actualDecompressConcatenated?
    try {

      new CompressorStreamFactory().createCompressorInputStream(
        CompressorStreamFactory.detect(bufferedInputStream),
        bufferedInputStream,
        true
      )

    } catch {

      case _: CompressorException =>
        System.err.println(s"[WARN] No compression found for input stream - raw input")
        bufferedInputStream

      case unknown: Throwable => println("[ERROR] Unknown exception: " + unknown)
        bufferedInputStream
    }
  }

  def compress(outputCompression: String, output: File): OutputStream = {
    try {
      // file is created here
      val myOutputStream = new FileOutputStream(output.toJava)
      outputCompression match {
        case "bz2" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, myOutputStream)

        case "gz" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream)

        case "deflate" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE, myOutputStream)

        case "lzma" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZMA, myOutputStream)

        case "sz" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, myOutputStream)

        case "xz" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.XZ, myOutputStream)

        case "zstd" =>
          new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, myOutputStream)

        case "" =>
          myOutputStream //if outputCompression is empty

        //        case "lz4-block" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_BLOCK, myOutputStream)
        //        case "lz4-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_FRAMED, myOutputStream)
        //        case "pack200" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.PACK200, myOutputStream)
        //        case "snappy-raw" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_RAW, myOutputStream)
      }
    } catch {
      case _: InvalidFormatException =>
        LoggerFactory.getLogger("CompressorLogger").error(s"InvalidFormat $outputCompression")
        new FileOutputStream(output.toJava)
    }
  }

  private def convertFormat(file: File, inputFormat: String, outputFormat: String): File = {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${file.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val data = readTriples(file, inputFormat, spark: SparkSession)

//    data.foreach(println(_))
    writeTriples(file, data, outputFormat, spark)
  }

  def readTriples(inputFile: File, inputFormat: String, spark: SparkSession): RDD[Triple] = {


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

      case "jsonl" =>
        try { //Mehrere Objekte pro Datei
          JSONL_Reader.readJSONL(spark, inputFile)
        } catch {
          case _: SparkException =>
            println("Json Object ueber mehrere Zeilen")
            RDF_Reader.read(spark, inputFile)
        }

      case "tsv" =>
        val mappingFile = scala.io.StdIn.readLine("Please type Path to Mapping File:\n")
        mappings.TSV_Reader.csv_to_rdd(mappingFile, inputFile.pathAsString, '\t', sc = spark.sparkContext)

      case "csv" =>
        val mappingFile:String = scala.io.StdIn.readLine("Please type Path to Mapping File:\n")
        val delimiter = scala.io.StdIn.readLine("Please type delimiter of CSV file:\n").toCharArray.apply(0).asInstanceOf[Character]
        val quotation = scala.io.StdIn.readLine("Please type quote Charater of CSV file:\n(e.x. ' \" ' for double quoted entries or ' null ' if there's no quotation)\n")
        val quoteChar = quotation match {
          case "null" => null
          case _ => quotation.toCharArray.apply(0).asInstanceOf[Character]
        }
        mappings.TSV_Reader.csv_to_rdd(mappingFile, inputFile.pathAsString, delimiter, quoteChar, spark.sparkContext)
    }
  }

  def writeTriples(inputFile: File, data: RDD[Triple], outputFormat: String, spark: SparkSession): File = {

    val tempDir = File("./target/databus.tmp/temp/")
    val mappingFile = tempDir / "mappingFile"
    if (tempDir.exists) tempDir.delete()
    val targetFile: File = tempDir / inputFile.nameWithoutExtension.concat(s".$outputFormat")

    outputFormat match {
      case "nt" =>
        NTriple_Writer.convertToNTriple(data).saveAsTextFile(tempDir.pathAsString)

      case "tsv" =>
        val createMappingFile = scala.io.StdIn.readLine("Type 'y' or 'yes' if you want to create a MappingFile.\n")

        if (createMappingFile.matches("yes|y")) {
          File("./mappings/").createDirectoryIfNotExists()

          val tsvData = TSV_Writer.convertToTSV(data, spark, createMappingFile = true)
          tsvData._1.coalesce(1).write
            .option("delimiter", "\t")
            .option("emptyValue", "")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "false")
            .csv(tempDir.pathAsString)

          TSV_Writer.createTarqlMapFile(tsvData._2, mappingFile)
        }

        else {
          val tsvData = TSV_Writer.convertToTSV(data, spark)
          tsvData.coalesce(1).write
            .option("delimiter", "\t")
            .option("emptyValue", "")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "false")
            .csv(tempDir.pathAsString)
        }


      case "ttl" =>
        TTL_Writer.convertToTTL(data, spark).coalesce(1).saveAsTextFile(tempDir.pathAsString)

      case "jsonld" =>
        JSONLD_Writer.convertToJSONLD(data, spark).saveAsTextFile(tempDir.pathAsString)

      case "rdfxml" =>
        RDF_Writer.convertToRDF(data, spark, RDFFormat.RDFXML).coalesce(1).saveAsTextFile(tempDir.pathAsString)
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

}
