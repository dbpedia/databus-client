package org.dbpedia.databus


import java.io.{BufferedInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.file.NoSuchFileException

import scala.language.postfixOps
import better.files.File
import net.sansa_stack.rdf.spark.io.RDFWriter
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.rdf_writer.{JSONLD_Writer, RDFXML_Writer, TSV_Writer, TTL_Writer}
import org.dbpedia.databus.rdf_reader.{JSONL_Reader, NTriple_Reader, RDF_Reader}


object Converter {

  def decompress(bufferedInputStream: BufferedInputStream): InputStream = {
    try {
      val compressorIn: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
      return compressorIn
    }
    catch {
      case noCompression: CompressorException => return bufferedInputStream
      case inInitializerError: ExceptionInInitializerError => return bufferedInputStream
      case noClassDefFoundError: NoClassDefFoundError => return bufferedInputStream
    }
  }

  def convertFormat(inputFile: File, inputFormat:String, outputFormat: String): File = {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparkContext = spark.sparkContext
//    sparkContext.setLogLevel("WARN")

    val data = inputFormat match {
      case "nt" => NTriple_Reader.readNTriples(spark,inputFile)
      case "rdf" => RDF_Reader.readRDF(spark, inputFile)
      case "ttl" => RDF_Reader.readRDF(spark, inputFile)
      case "jsonld" => RDF_Reader.readRDF(spark, inputFile) //Ein Objekt pro Datei
//      } catch {
//        case noSuchMethodError: NoSuchMethodError => {
//          println("Json Object ueber mehrere Zeilen")
//          JSONL_Reader.readJSONL(spark, inputFile)
//        }
//      }
      case "jsonl" => try {   //Mehrere Objekte pro Datei
        JSONL_Reader.readJSONL(spark, inputFile)
      } catch {
        case onlyOneJsonObject: SparkException => {
          println("Json Object ueber mehrere Zeilen")
          RDF_Reader.readRDF(spark, inputFile)
        }
      }
    }

    val tempDir = inputFile.parent / "temp"
    val headerTempDir = inputFile.parent / "tempheader"

    val targetFile: File = tempDir / inputFile.nameWithoutExtension.concat(s".$outputFormat")

    //delete temp directory if exists
    try {
      tempDir.delete()
    } catch {
      case noFile: NoSuchFileException => ""
    }

    outputFormat match {
      case "nt" => data.saveAsNTriplesFile(tempDir.pathAsString)
      case "tsv" => {
        val solution = TSV_Writer.convertToTSV(data, spark)
        solution(1).write.option("delimiter", "\t").option("nullValue","?").option("treatEmptyValuesAsNulls", "true").csv(tempDir.pathAsString)
        solution(0).write.option("delimiter", "\t").csv(headerTempDir.pathAsString)
      }
      case "ttl" => TTL_Writer.convertToTTL(data, spark).coalesce(1).saveAsTextFile(tempDir.pathAsString)
      case "jsonld" => JSONLD_Writer.convertToJSONLD(data).saveAsTextFile(tempDir.pathAsString)
      case "rdfxml" => RDFXML_Writer.convertToRDFXML(data, spark).coalesce(1).saveAsTextFile(tempDir.pathAsString)
    }

    try {
      outputFormat match {
        case "tsv" => FileHandler.unionFilesWithHeaderFile(headerTempDir, tempDir, targetFile)
        case "jsonld" | "jsonl" | "nt" | "ttl" | "rdfxml" => FileHandler.unionFiles(tempDir, targetFile)
      }
    }
    catch {
      case fileAlreadyExists: RuntimeException => deleteAndRestart(inputFile ,inputFormat, outputFormat, targetFile: File)
    }


    return targetFile
  }


  def deleteAndRestart(inputFile: File, inputFormat:String, outputFormat: String, file: File): Unit = {
    file.delete()
    convertFormat(inputFile, inputFormat, outputFormat)
  }

  def compress(outputCompression: String, output: File): OutputStream = {
    try {
      // file is created here
      val myOutputStream = new FileOutputStream(output.toJava)
      val out: OutputStream = outputCompression match {
        case "bz2" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, myOutputStream)
        case "gz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream)
        case "deflate" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE, myOutputStream)
//        case "lz4-block" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_BLOCK, myOutputStream)
//        case "lz4-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_FRAMED, myOutputStream)
        case "lzma" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZMA, myOutputStream)
//        case "pack200" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.PACK200, myOutputStream)
        case "snappy-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, myOutputStream)
//        case "snappy-raw" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_RAW, myOutputStream)
        case "xz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.XZ, myOutputStream)
        case "zstd" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, myOutputStream)
        case "" => myOutputStream //if outputCompression is empty
      }
      return out
    }
  }

}