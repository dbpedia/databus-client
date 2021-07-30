package org.dbpedia.databus.client.filehandling.convert.compression

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream, OutputStream}
import better.files.File
import org.apache.commons.compress.archivers.dump.InvalidFormatException
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.slf4j.LoggerFactory

object Compressor {

  def decompress(file:File): InputStream = {
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(file.toJava))
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

  def compress(output: File, outputCompression: String): OutputStream = {
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


}
