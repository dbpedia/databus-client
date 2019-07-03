package org.dbpedia.databus

import java.io.{BufferedInputStream, FileOutputStream, InputStream, OutputStream}
import scala.sys.process._
import scala.language.postfixOps
import scala.util.control.Breaks._
import scala.io.Source
import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.io.FileUtils
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._


object Converter {

  def getCompressionType(fileInputStream: BufferedInputStream): String ={

    try{
      var ctype = CompressorStreamFactory.detect(fileInputStream)
      if (ctype == "bzip2"){
        ctype="bz2"
      }
      return ctype
    }
    catch{
      case noCompression: CompressorException => ""
    }
  }

  def getFormatType(inputFile: File): String ={

    // Suche in Dataid.ttl nach allen Zeilen die den Namen der Datei enthalten
    val lines = Source.fromFile((inputFile.parent / "dataid.ttl").pathAsString).getLines().filter(_ contains s"${inputFile.name}")

    val regex = s"<\\S*dataid.ttl#${inputFile.name}\\S*>".r
    var fileURL = ""

    for(line<-lines){
      breakable {
        for(x<-regex.findAllMatchIn(line)) {
          fileURL = x.toString().replace(">","").replace("<","")
          break
        }
      }
    }


    val fileType = QueryHandler.getTypeOfFile(fileURL, inputFile.parent / "dataid.ttl")
    return fileType
  }

  def decompress(bufferedInputStream: BufferedInputStream): InputStream = {
    try {
      val compressorIn: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
      return compressorIn
    }
    catch{
      case noCompression: CompressorException => bufferedInputStream
    }
  }

//  def handleNoCompressorException(fileInputStream: FileInputStream): BufferedInputStream ={
//    new BufferedInputStream(fileInputStream)
//  }


  def convertFormat(inputFile: File, outputFormat:String): File= {

    val outputFormat = "nt"

    val spark = SparkSession.builder().master("local").getOrCreate()
    val data = NTripleReader.load(spark, inputFile.pathAsString)
    val tempDir = s"${inputFile.parent.pathAsString}/temp"
    val targetFile:File = inputFile.parent / inputFile.nameWithoutExtension.concat(s".$outputFormat")

    try {
      data.saveAsNTriplesFile(tempDir)

      val findTripleFiles = s"find $tempDir/ -name part*" !!
      val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

      if( concatFiles == 0 ) FileUtils.deleteDirectory(File(tempDir).toJava)
      else System.err.println(s"[WARN] failed to merge $tempDir/*")
    }
    catch {
      case fileAlreadyExists: RuntimeException => deleteAndRestart(inputFile: File, outputFormat: String, targetFile: File)
    }
    return targetFile
  }

  def deleteAndRestart(inputFile:File , outputFormat:String, file: File): Unit ={
    file.delete()
    convertFormat(inputFile, outputFormat)
  }

//  def convertFormat(inputFile: File, outputFormat:String): File= {
//
//    val spark = SparkSession.builder
//      .appName(s"Triple reader  ${inputFile.name}")
//      .master("local[*]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .getOrCreate()
//
//    val lang = Lang.NTRIPLES
//    val triples = spark.rdf(lang)(inputFile.pathAsString)
//
//    val tempDir = s"${inputFile.parent.pathAsString}/temp"
//
//    val targetFile:File = inputFile.parent / inputFile.nameWithoutExtension.concat(s".$outputFormat")
//    println(targetFile.pathAsString)
//
//
//    //    val triples = spark.rdf(lang)(filePath)
//    //
//    //    triples.take(5).foreach(println(_))
//
//    return targetFile
//  }


  def compress(outputCompression:String, output:File): OutputStream = {
    try {
      // file is created here
      val myOutputStream = new FileOutputStream(output.toJava)
      val out: OutputStream = outputCompression match{
        case "bz2" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, myOutputStream)
        case "gz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream)
        case "br" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BROTLI, myOutputStream)
        case "deflate" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE, myOutputStream)
        case "deflate64" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE64, myOutputStream)
        case "lz4-block" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_BLOCK, myOutputStream)
        case "lz4-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_FRAMED, myOutputStream)
        case "lzma" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZMA, myOutputStream)
        case "pack200" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.PACK200, myOutputStream)
        case "snappy-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, myOutputStream)
        case "snappy-raw" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_RAW, myOutputStream)
        case "xz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.XZ, myOutputStream)
        case "z" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.Z, myOutputStream)
        case "zstd" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, myOutputStream)
        case "" => myOutputStream //if outputCompression is empty
      }
      return out
    }
  }

}
