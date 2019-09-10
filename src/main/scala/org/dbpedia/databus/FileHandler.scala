package org.dbpedia.databus

import java.io.{BufferedInputStream, FileInputStream, FileNotFoundException, FileOutputStream, InputStream, OutputStream}
import java.net.URL
import java.nio.file.NoSuchFileException

import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.dbpedia.databus.sparql.QueryHandler

import scala.io.Source
import scala.sys.process._
import scala.util.control.Breaks.{break, breakable}

object FileHandler {

  def convertFile(inputFile:File, src_dir:File, dest_dir:File, outputFormat:String, outputCompression:String): Unit = {
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(inputFile.toJava))

    val compressionInputFile = getCompressionType(bufferedInputStream)
    val formatInputFile = getFormatType(inputFile,compressionInputFile)

    if (outputCompression==compressionInputFile && (outputFormat==formatInputFile || outputFormat=="same")){
      val outputStream = new FileOutputStream(getOutputFile(inputFile, formatInputFile, compressionInputFile, src_dir, dest_dir).toJava)
      copyStream(new FileInputStream(inputFile.toJava), outputStream)
    }
    else if (outputCompression!=compressionInputFile && (outputFormat==formatInputFile || outputFormat=="same")){
      val decompressedInStream = Converter.decompress(bufferedInputStream)
      val compressedFile = getOutputFile(inputFile, formatInputFile, outputCompression, src_dir, dest_dir)
      val compressedOutStream = Converter.compress(outputCompression, compressedFile)
      //file is written here
      copyStream(decompressedInStream, compressedOutStream)
    }
    //  With FILEFORMAT CONVERSION
    else{

      formatInputFile match {
          case "rdf" | "ttl" | "nt" | "jsonld" =>
          case _ => {
            println("Output file format not supported.")
            return Unit
          }
      }

      val targetFile = getOutputFile(inputFile, outputFormat, outputCompression, src_dir, dest_dir)
      var typeConvertedFile = File("")

      if(!(compressionInputFile=="")){
        val decompressedInStream = Converter.decompress(bufferedInputStream)
        val decompressedFile = inputFile.parent / inputFile.nameWithoutExtension(true).concat(s".$formatInputFile")
        copyStream(decompressedInStream, new FileOutputStream(decompressedFile.toJava))
        typeConvertedFile = Converter.convertFormat(decompressedFile, formatInputFile, outputFormat)
        decompressedFile.delete()
      }
      else{
        typeConvertedFile = Converter.convertFormat(inputFile, formatInputFile, outputFormat)
      }

      val compressedOutStream = Converter.compress(outputCompression, targetFile)
      //file is written here
      copyStream(new FileInputStream(typeConvertedFile.toJava), compressedOutStream)

      try {
        typeConvertedFile.parent.delete()
      }
      catch {
        case noSuchFileException: NoSuchFileException => ""
      }
    }

  }

  def getOutputFile(inputFile: File, outputFormat:String, outputCompression:String,src_dir: File, dest_dir: File): File ={

    val nameWithoutExtension = inputFile.nameWithoutExtension
    val name = inputFile.name
    var filepath_new = ""
    val dataIdFile = inputFile.parent / "dataid.ttl"

    val newOutputFormat={
      if (outputFormat=="rdfxml") "rdf"
      else outputFormat
    }

    if(dataIdFile.exists) {
      val dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
      filepath_new = dest_dir.pathAsString.concat("/")
      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
      filepath_new = filepath_new.concat(nameWithoutExtension)
    }
    else{
      // changeExtensionTo() funktioniert nicht bei noch nicht existierendem File, deswegen ausweichen über Stringmanipulation
      filepath_new = inputFile.pathAsString.replaceAll(src_dir.pathAsString,dest_dir.pathAsString.concat("/NoDataID"))
      filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
    }

    if (outputCompression.isEmpty){
      filepath_new = filepath_new.concat(".").concat(newOutputFormat)
    }
    else{
      filepath_new = filepath_new.concat(".").concat(newOutputFormat).concat(".").concat(outputCompression)
    }

    val outputFile = File(filepath_new)
    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    println(s"converted file:\t${outputFile.pathAsString}\n")

    return outputFile
  }

  def copyStream(in: InputStream, out: OutputStream): Unit ={
    try {
      IOUtils.copy(in, out)
    }
    finally if (out != null) {
      out.close()
    }
  }

  def readQueryFile(file:File): String = {
    var queryString:String = ""
    for (line <- file.lineIterator) {
      queryString = queryString.concat(line).concat("\n")
    }
    return queryString
  }

  def downloadFile(url: String, targetdir:File): Unit = {
    println(url)
    val file = targetdir / url.split("http://|https://").map(_.trim).last //filepath from url without http://

    file.parent.createDirectoryIfNotExists(createParents = true)
    FileUtils.copyURLToFile(new URL(url),file.toJava)

    val dataIdFile = file.parent / "dataid.ttl"
    if (!dataIdFile.exists()){  //if no dataid.ttl File in directory of downloaded file, then download the belongig dataid.ttl
//      println("Download Dataid.ttl")
      QueryHandler.getDataIdFile(url ,dataIdFile)
    }
  }

  def getCompressionType(fileInputStream: BufferedInputStream): String = {
    try {
      var ctype = CompressorStreamFactory.detect(fileInputStream)
      if (ctype == "bzip2") {
        ctype = "bz2"
      }
      return ctype
    }
    catch {
      case noCompression: CompressorException => ""
      case inInitializerError: ExceptionInInitializerError => ""
      case noClassDefFoundError: NoClassDefFoundError => ""
    }
  }

  def copyUnchangedFile(inputFile:File , src_dir:File, dest_dir:File)={
    val name = inputFile.name
    var filepath_new = ""
    val dataIdFile = inputFile.parent / "dataid.ttl"

    if(dataIdFile.exists) {
      val dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
      filepath_new = dest_dir.pathAsString.concat("/")
      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
      filepath_new = filepath_new.concat(name)
    }
    else{
      filepath_new = inputFile.pathAsString.replaceAll(src_dir.pathAsString,dest_dir.pathAsString.concat("/NoDataID"))
    }

    val outputFile = File(filepath_new)

    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    val outputStream = new FileOutputStream(outputFile.toJava)
    copyStream(new FileInputStream(inputFile.toJava), outputStream)
  }

  def getFormatType(inputFile:File, compressionInputFile:String)={
    {
      try {
        if(!(getFormatTypeWithDataID(inputFile) == "")){
          getFormatTypeWithDataID(inputFile)
        } else {
          getFormatTypeWithoutDataID(inputFile, compressionInputFile)
        }
      } catch {
        case fileNotFoundException: FileNotFoundException => getFormatTypeWithoutDataID(inputFile, compressionInputFile)
      }
    }
  }

  def getFormatTypeWithDataID(inputFile: File): String = {
    // Suche in Dataid.ttl nach allen Zeilen die den Namen der Datei enthalten
    val lines = Source.fromFile((inputFile.parent / "dataid.ttl").toJava, "UTF-8").getLines().filter(_ contains s"${inputFile.name}")

    val regex = s"<\\S*dataid.ttl#${inputFile.name}\\S*>".r
    var fileURL = ""

    for (line <- lines) {
      breakable {
        for (x <- regex.findAllMatchIn(line)) {
          fileURL = x.toString().replace(">", "").replace("<", "")
          break
        }
      }
    }

    val fileType = QueryHandler.getTypeOfFile(fileURL, inputFile.parent / "dataid.ttl")
    return fileType
  }

  def getFormatTypeWithoutDataID(inputFile: File, compression: String): String = {
    var split = inputFile.name.split("\\.")
    var fileType = ""

    if (compression == ""){
      fileType = split(split.size-1)
    } else {
      fileType = split(split.size-2)
    }

    return fileType
  }

  def unionFiles(tempDir:File, targetFile:File)={
    //union all part files of Sansa

    //HOW TO ESCAPE WHITESPACES?
    val findTripleFiles = s"find ${tempDir.pathAsString}/ -name part* -not -empty" !!
    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

    if (! (concatFiles == 0) ) {
      System.err.println(s"[WARN] failed to merge ${tempDir.pathAsString}/*")
    }
  }

  def unionFilesWithHeaderFile(headerTempDir:File, tempDir:File, targetFile:File)={
    //union all part files of Sansa

    val findTripleFiles = s"find ${headerTempDir.pathAsString}/ -name part*" #&& s"find ${tempDir.pathAsString}/ -name part*" !!
    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

    if( concatFiles == 0 ){
      FileUtils.deleteDirectory(headerTempDir.toJava)
    }
    else System.err.println(s"[WARN] failed to merge ${tempDir.pathAsString}/*")

  }

//  def deleteDataIdFiles(dir:File, dataIdString:String) ={
//    val files = dir.listRecursively.toSeq
//    for (file <- files) {
//      if (! file.isDirectory){
//        if (file.name.equals(dataIdString)){
//          println(s"dataid file:\t\t${file.pathAsString}")
//          file.delete()
//        }
//      }
//      else if (file.name == "temp") { //Delete temp dir of previous failed run
//        file.delete()
//      }
//    }
//  }
}
