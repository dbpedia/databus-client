package org.dbpedia.databus

import java.io.{BufferedInputStream, FileOutputStream, InputStream, OutputStream}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.streaming.StreamReader
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


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

  def getFormatType(in: InputStream): String ={
    "ttl"
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

  def convertFormat(input: InputStream, outputFormat:String)={
//    val convertedStream = input

//    val spark= SparkSession.builder.
//      master("local")
//      .appName("spark session test")
//      .getOrCreate()

//    val df = spark.read
//      .format("csv")
//      .option("header", "true") //first line in file has headers
//      .load("./downloaded_files/New Folder/geo-coordinates-mappingbased_lang=ca.ttl")


//    val lang = Lang.NTRIPLES
//    new StreamReader {
//      override def load(ssc: StreamingContext): DStream[graph.Triple] = ???
//    }
//    val triples = spark.rdf(lang)("./downloaded_files/dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.ttl.bz2")
////    "./downloaded_files/dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.ttl.bz2"
//
//    triples.take(5).foreach(println(_))
//
//
//
//    val df =  spark.rdf(lang)
//    val someRDD = df.rdd
//    val newDF = spark.createDataFrame(someRDD, df.schema)
//
////    val args = Array("") //./downloaded_files/New Folder/test.csv
////    TripleReader.main(args)

    input
  }

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
