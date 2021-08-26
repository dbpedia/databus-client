package org.dbpedia.databus.client.filehandling

import java.io._
import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.dbpedia.databus.client.filehandling.FileUtil.copyStream
import org.dbpedia.databus.client.filehandling.convert.FormatConverter
import org.dbpedia.databus.client.filehandling.convert.compression.Compressor
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.io.Source

class FileHandler(cliConfig: CLI_Config) {

  /**
    * handle input file
    *
    * @param inputFile input file
    */
  def handleFile(inputFile:File): Option[File] = {

    println(s"input file:\t${inputFile.pathAsString}")

    val inCompression = FileUtil.getCompressionType(inputFile)
    val inFormat = FileUtil.getFormatType(inputFile, inCompression)

    val config:CompileConfig = new CompileConfig(
      inputFormat = inFormat,
      inputCompression = inCompression,
      outputFormat = {
        if (cliConfig.format()=="same") inFormat
        else cliConfig.format() },
      outputCompression = {
        if(cliConfig.compression()=="same") inCompression
        else cliConfig.compression() },
      target = File(cliConfig.target()),
      mapping = cliConfig.mapping(),
      delimiter = cliConfig.delimiter().toCharArray.head,
      quotation = cliConfig.quotation().toCharArray.head,
      createMapping = cliConfig.createMapping(),
      graphURI = cliConfig.graphURI(),
      outFile = getOutputFile(inputFile)
    )

    // Without any Conversion
    if ((config.inputCompression == config.outputCompression) && (config.inputFormat == config.outputFormat)) {
      copyStream(new FileInputStream(inputFile.toJava), new FileOutputStream(config.outFile.toJava))
      Some(config.outFile)
    }
    // Only Compression Conversion
    else if (config.inputCompression != config.outputCompression && (config.inputFormat == config.outputFormat)) {
      copyStream(Compressor.decompress(inputFile), Compressor.compress(config.outFile, config.outputCompression))
      Some(config.outFile)
    }

    // File Format Conversion (need to uncompress anyway)
    else {
      if (!isSupportedInFormat(config.inputFormat)) return None

      config.sha = {
        if (FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt")) != "") FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt"))
        else FileUtil.getSha256(inputFile)
      }

      val formatConvertedData = {
        if (!(config.inputCompression == "")) {
          val decompressedInStream = Compressor.decompress(inputFile)
          val decompressedFile = File("./target/databus.tmp/") / inputFile.nameWithoutExtension(true).concat(s".${config.inputFormat}")
          copyStream(decompressedInStream, new FileOutputStream(decompressedFile.toJava))
          FormatConverter.convert(decompressedFile, config)
        }
        else {
          FormatConverter.convert(inputFile, config)
        }
      }

      if (formatConvertedData.isDirectory){
        config.outFile.createDirectoryIfNotExists()
        val formatConvertedFiles = formatConvertedData.children
        while(formatConvertedFiles.hasNext) {
          val formatConvertedFile = formatConvertedFiles.next()
          val newOutFile = {
            if (config.outputCompression.nonEmpty) config.outFile / s"${formatConvertedFile.name}.${config.outputCompression}"
            else config.outFile / s"${formatConvertedFile.name}"
          }
          val compressedOutStream = Compressor.compress(newOutFile, config.outputCompression)
          copyStream(new FileInputStream(formatConvertedFile.toJava), compressedOutStream)
        }
      } else {
        val compressedOutStream = Compressor.compress(config.outFile, config.outputCompression)
        copyStream(new FileInputStream(formatConvertedData.toJava), compressedOutStream)
      }

      //DELETE TEMPDIR
      //      if (typeConvertedFile.parent.exists) typeConvertedFile.parent.delete()
      Some(config.outFile)
    }

  }



  /**
    * checks if a desired format is supported by the Databus Client
    *
    * @param format desired format
    * @return true, if it is supported
    */
  def isSupportedInFormat(format: String): Boolean = {
    if (format.matches("rdf|ttl|nt|jsonld|tsv|csv|nq|trix|trig")) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Input file format $format is not supported.")
      println(s"Input file format $format is not supported.")
      false
    }
  }

  /**
   * calculate output file depending on input file and desired output compression and format
   *
   * @param inputFile input file
   * @return output file
   */
  def getOutputFile(inputFile: File): File = {

    val nameWithoutExtension = inputFile.nameWithoutExtension

    val dataIdFile = inputFile.parent / "dataid.ttl"

    val target_dir = File(cliConfig.target())

    val newOutputFormat = {
      if (cliConfig.format() == "rdfxml") "rdf"
      else cliConfig.format()
    }

    val outputDir = {
      if (dataIdFile.exists) {
        val pgav = QueryHandler.getTargetDir(dataIdFile)
        val fw = new FileWriter((target_dir / "identifiers_downloadedFiles.txt").pathAsString, true)
        try {
          fw.append(s"https://databus.dbpedia.org/$pgav/${inputFile.name}\n")
        }
        finally fw.close()

        File(s"${target_dir.pathAsString}/$pgav")
      }
      else
        File(target_dir.pathAsString.concat("/NoDataID")
          .concat(inputFile.pathAsString.splitAt(inputFile.pathAsString.lastIndexOf("/"))._1
            .replace(File(".").pathAsString, "")
          )
        )
    }

    val newName = {
      if (cliConfig.compression().isEmpty) s"$nameWithoutExtension.$newOutputFormat"
      else s"$nameWithoutExtension.$newOutputFormat.${cliConfig.compression()}"
    }

    val outputFile = outputDir / newName

    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    println(s"output file:\t${outputFile.pathAsString}\n")

    outputFile
  }
}
