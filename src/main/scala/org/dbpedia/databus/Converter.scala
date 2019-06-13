package org.dbpedia.databus

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream, OutputStream}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorOutputStream, CompressorStreamFactory}
import org.apache.commons.io.IOUtils


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


  def convertFormat(input: InputStream, outputFormat:String)={
    val convertedStream = input
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
