package org.dbpedia.databus.client.filehandling

import better.files.File
import org.dbpedia.databus.client.filehandling.FileUtil.copyStream
import org.dbpedia.databus.client.filehandling.convert.Converter
import org.dbpedia.databus.client.filehandling.convert.compression.Compressor
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import java.io._

class FileHandler(cliConfig: CLI_Config) {

  /**
    * handle input file
    *
    * @param inputFile input file
    */
  def handleFile(inputFile:File): Option[File] = {

    println(s"input file:\t${inputFile.pathAsString}")

    val config:CompileConfig = new CompileConfig(inputFile,cliConfig).init()

    // Without any Conversion
    if ((config.inCompression == config.outCompression) && (config.inFormat == config.outFormat)) {
      copyStream(new FileInputStream(inputFile.toJava), new FileOutputStream(config.outFile.toJava))
      Some(config.outFile)
    }
    // Only Compression Conversion
    else if (config.inCompression != config.outCompression && (config.inFormat == config.outFormat)) {
      copyStream(Compressor.decompress(inputFile), Compressor.compress(config.outFile, config.outCompression))
      Some(config.outFile)
    }
    // File Format Conversion (need to uncompress anyway)
    else {
      if (!isSupportedInFormat(config.inFormat)) return None

      val formatConvertedData = {
        if (!(config.inCompression == "")) {
          val decompressedInStream = Compressor.decompress(inputFile)
          val decompressedFile = File("./target/databus.tmp/") / inputFile.nameWithoutExtension(true).concat(s".${config.inFormat}")
          copyStream(decompressedInStream, new FileOutputStream(decompressedFile.toJava))
          Converter.convert(decompressedFile, config)
        }
        else {
          Converter.convert(inputFile, config)
        }
      }

      try{
        if (formatConvertedData.isDirectory){
          config.outFile.createDirectoryIfNotExists()
          val formatConvertedFiles = formatConvertedData.children
          while(formatConvertedFiles.hasNext) {
            val formatConvertedFile = formatConvertedFiles.next()
            val newOutFile = {
              if (config.outCompression.nonEmpty) config.outFile / s"${formatConvertedFile.name}.${config.outCompression}"
              else config.outFile / s"${formatConvertedFile.name}"
            }
            val compressedOutStream = Compressor.compress(newOutFile, config.outCompression)
            copyStream(new FileInputStream(formatConvertedFile.toJava), compressedOutStream)
          }
        } else {
          val compressedOutStream = Compressor.compress(config.outFile, config.outCompression)
          copyStream(new FileInputStream(formatConvertedData.toJava), compressedOutStream)
        }

        //DELETE TEMPDIR
        //      if (typeConvertedFile.parent.exists) typeConvertedFile.parent.delete()
        Some(config.outFile)
      } catch {
        case nullPointerException: NullPointerException => null
      }


    }

  }


  /**
   * checks if a desired format is supported by the Databus Client
   *
   * @param format desired format
   * @return true, if it is supported
   */
  def isSupportedInFormat(format: String): Boolean = {
    if (format.matches(Config.fileFormats)) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Input file format $format is not supported.")
      println(s"Input file format $format is not supported.")
      false
    }
  }


}
