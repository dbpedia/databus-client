package org.dbpedia.databus.filehandling

import java.io.{BufferedInputStream, FileInputStream, FileNotFoundException, FileOutputStream, FileWriter}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.dbpedia.databus.filehandling.FileUtil.copyStream
import org.dbpedia.databus.filehandling.convert.compression.Compressor
import org.dbpedia.databus.filehandling.convert.format.Converter
import org.dbpedia.databus.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.io.Source

object FileHandler
{
  def handleFile(inputFile:File, dest_dir: File, outputFormat: String, outputCompression: String): Unit = {

    println(s"input file:\t\t${inputFile.pathAsString}")
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(inputFile.toJava))

    val compressionInputFile = getCompressionType(bufferedInputStream)
    val formatInputFile = getFormatType(inputFile, compressionInputFile)

    if ((outputCompression == compressionInputFile || outputCompression == "same") && (outputFormat == formatInputFile || outputFormat == "same")) {
      val outputStream = new FileOutputStream(getOutputFile(inputFile, formatInputFile, compressionInputFile, dest_dir).toJava)
      copyStream(new FileInputStream(inputFile.toJava), outputStream)
    }

    else if (outputCompression != compressionInputFile && (outputFormat == formatInputFile || outputFormat == "same")) {
      val decompressedInStream = Compressor.decompress(bufferedInputStream)
      val compressedFile = getOutputFile(inputFile, formatInputFile, outputCompression, dest_dir)
      val compressedOutStream = Compressor.compress(outputCompression, compressedFile)
      copyStream(decompressedInStream, compressedOutStream)
    }

    //  With FILEFORMAT CONVERSION
    else {

      if (!isSupportedInFormat(formatInputFile)) return

      val newOutCompression = {
        if (outputCompression == "same") compressionInputFile
        else outputCompression
      }

      val targetFile = getOutputFile(inputFile, outputFormat, newOutCompression, dest_dir)
      var typeConvertedFile = File("")

      val sha = {
        if (FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt")) != "") FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt"))
        else FileUtil.getSha256(inputFile)
      }



      if (!(compressionInputFile == "")) {
        val decompressedInStream = Compressor.decompress(bufferedInputStream)
        val decompressedFile = File("./target/databus.tmp/") / inputFile.nameWithoutExtension(true).concat(s".$formatInputFile")
        copyStream(decompressedInStream, new FileOutputStream(decompressedFile.toJava))
        typeConvertedFile = Converter.convertFormat(decompressedFile, formatInputFile, outputFormat, sha)
      }
      else {
        typeConvertedFile = Converter.convertFormat(inputFile, formatInputFile, outputFormat, sha)
      }

      val compressedOutStream = Compressor.compress(newOutCompression, targetFile)
      copyStream(new FileInputStream(typeConvertedFile.toJava), compressedOutStream)

      //DELETE TEMPDIR
      //      if (typeConvertedFile.parent.exists) typeConvertedFile.parent.delete()

    }

  }

  def getOutputFile(inputFile: File, outputFormat: String, outputCompression: String, dest_dir: File): File = {

    val nameWithoutExtension = inputFile.nameWithoutExtension

    val dataIdFile = inputFile.parent / "dataid.ttl"

    val newOutputFormat = {
      if (outputFormat == "rdfxml") "rdf"
      else outputFormat
    }

    val outputDir = {
      if (dataIdFile.exists) {
        val pgav = QueryHandler.getTargetDir(dataIdFile)
        val fw = new FileWriter((dest_dir / "identifiers_downloadedFiles.txt").pathAsString, true)
        try {
          fw.append(s"https://databus.dbpedia.org/$pgav/${inputFile.name}\n")
        }
        finally fw.close()

        File(s"${dest_dir.pathAsString}/$pgav")
      }
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

  def isSupportedInFormat(format: String): Boolean = {
    if (format.matches("rdf|ttl|nt|jsonld|tsv|csv")) true
    else {
      LoggerFactory.getLogger("File Format Logger").error(s"Input file format $format is not supported.")
      println(s"Input file format $format is not supported.")
      false
    }
  }

  def getFormatType(inputFile: File, compressionInputFile: String): String = {
    {
      try {
        if (!(FileHandler.getFormatTypeWithDataID(inputFile) == "")) {
          FileHandler.getFormatTypeWithDataID(inputFile)
        } else {
          FileHandler.getFormatTypeWithoutDataID(inputFile, compressionInputFile)
        }
      } catch {
        case _: FileNotFoundException => FileHandler.getFormatTypeWithoutDataID(inputFile, compressionInputFile)
      }
    }
  }

  //SIZE DURCH LENGTH ERSETZEN
  def getFormatTypeWithoutDataID(inputFile: File, compression: String): String = {
    val split = inputFile.name.split("\\.")

    if (compression == "") split(split.size - 1)
    else split(split.size - 2)
  }

  def getFormatTypeWithDataID(inputFile: File): String = {
    // Suche in Dataid.ttl nach allen Zeilen die den Namen der Datei enthalten
    val source = Source.fromFile((inputFile.parent / "dataid.ttl").toJava, "UTF-8")
    val lines = source.getLines().filter(_ contains s"${inputFile.name}")

    val regex = s"<\\S*dataid.ttl#${inputFile.name}\\S*>".r
    var fileURL = ""

    import scala.util.control.Breaks.{break, breakable}

    for (line <- lines) {
      breakable {
        for (x <- regex.findAllMatchIn(line)) {
          fileURL = x.toString().replace(">", "").replace("<", "")
          break
        }
      }
    }

    source.close()
    QueryHandler.getFileExtension(fileURL, inputFile.parent / "dataid.ttl")
  }

  def getCompressionType(fileInputStream: BufferedInputStream): String = {
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

}
