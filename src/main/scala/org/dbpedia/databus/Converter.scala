package org.dbpedia.databus

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorOutputStream, CompressorStreamFactory}
import org.apache.commons.io.IOUtils


object Converter {

  def getCompressionType(file: File): String ={
    CompressorStreamFactory.detect(new BufferedInputStream(new FileInputStream(file.toJava)))
  }

  def decompress(file: File): InputStream = {
    try {
      //println(CompressorStreamFactory.detect(new BufferedInputStream(new FileInputStream(file.toJava))))
      val in: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(file.toJava)))
      return in
    }
    catch{
      case noCompressor: CompressorException => handleNoCompressorException(file)
    }
  }

  def handleNoCompressorException(file: File): BufferedInputStream ={
    new BufferedInputStream(new FileInputStream(file.toJava))
  }

  def convertFormat(input: InputStream, outputFormat:String)={
    val convertedStream = input
  }

  def compress(inputStream: InputStream, outputCompression:String, output:File) = {
    try {
      // file is created here
      val myOutputStream = new FileOutputStream(output.toJava)

      val out: CompressorOutputStream = outputCompression match{
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
      }

      try {
        IOUtils.copy(inputStream, out)
      }
      finally if (out != null) {
        out.close()
      }
    }
  }

}
