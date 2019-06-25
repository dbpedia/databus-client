package org.dbpedia.databus

import java.io.{BufferedInputStream, BufferedReader, FileOutputStream, InputStream, InputStreamReader, OutputStream}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import net.sansa_stack.rdf.spark.streaming.StreamReader
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
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

//  def handleNoCompressorException(fileInputStream: FileInputStream): BufferedInputStream ={
//    new BufferedInputStream(fileInputStream)
//  }


  def convertFormat(inputFile: File, outputFormat:String)={

    val lang = Lang.NTRIPLES
    new StreamReader {
      override def load(ssc: StreamingContext): DStream[graph.Triple] = ???
    }

    val spark = SparkSession.builder().master("local").getOrCreate()
    val data = NTripleReader.load(spark, inputFile.pathAsString)
    data.saveAsNTriplesFile("./test")

    val triples = spark.rdf(lang)(inputFile.pathAsString)

    triples.take(5).foreach(println(_))

  }


//  def convertFormat(input: InputStream, outputFormat:String)= {
//
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(conf, Seconds(10))
//
//    val inputstreamReceiver = ssc.receiverStream(new InputstreamReceiver(input))
//      .flatMap(_.split(" "))
//
//    inputstreamReceiver.foreachRDD(rdd => {
//      val topList = rdd.collect
//      println("\nLast minute popular domains (%s total):".format(rdd.count()))
//      topList.foreach { case (count) => println("%s (%s clicks)".format(count)) }
//    })
//
//    ssc.start()
//    ssc.stop(true, true)
//
//    input
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
