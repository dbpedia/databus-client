package org.dbpedia.databus

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.net.URL
import java.nio.file.NoSuchFileException
import better.files.File
import org.apache.commons.io.{FileUtils, IOUtils}
import scala.sys.process._

object FileHandler {

//  val src_dir  = "./downloaded_files/"

  def convertFile(inputFile:File, temp_dir:String, dest_dir:String, outputFormat:String, outputCompression:String): Unit = {
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(inputFile.toJava))
    val compressionInputFile = Converter.getCompressionType(bufferedInputStream)
    val formatInputFile = Converter.getFormatType(inputFile) //NOCH OHNE FUNKTION

    if (outputCompression=="same" && outputFormat=="same"){
      val outputStream = new FileOutputStream(getOutputFile(inputFile, formatInputFile, compressionInputFile, temp_dir, dest_dir).toJava)
      copyStream(new FileInputStream(inputFile.toJava), outputStream)
    }
    else if (outputCompression!="same" && outputFormat=="same"){
      val decompressedInStream = Converter.decompress(bufferedInputStream)
      val compressedFile = getOutputFile(inputFile, formatInputFile, outputCompression, temp_dir, dest_dir)
      val compressedOutStream = Converter.compress(outputCompression, compressedFile)
      //file is written here
      copyStream(decompressedInStream, compressedOutStream)
    }
    //  With FILEFORMAT CONVERSION
//      MUSS NOCHMAL UEBERARBEITET WERDEN
    else if (outputCompression=="same" && outputFormat!="same"){
      val targetFile = getOutputFile(inputFile, outputFormat, compressionInputFile, temp_dir, dest_dir)
      val typeConvertedFile = Converter.convertFormat(inputFile, outputFormat)
      val compressedOutStream = Converter.compress(compressionInputFile, targetFile)
      //file is written here
      copyStream(new FileInputStream(typeConvertedFile.toJava), compressedOutStream)
      typeConvertedFile.delete()
    }
    else{
      val targetFile = getOutputFile(inputFile, outputFormat, outputCompression, temp_dir, dest_dir)
      var typeConvertedFile = File("")

      if(!(compressionInputFile=="")){
        val decompressedInStream = Converter.decompress(bufferedInputStream)
        val decompressedFile = inputFile.parent / inputFile.nameWithoutExtension(true).concat(s".$formatInputFile")
        copyStream(decompressedInStream, new FileOutputStream(decompressedFile.toJava))
//        println(decompressedFile.pathAsString)
        typeConvertedFile = Converter.convertFormat(decompressedFile, outputFormat)
        decompressedFile.delete()
      }
      else{
        typeConvertedFile = Converter.convertFormat(inputFile, outputFormat)
      }

      val compressedOutStream = Converter.compress(outputCompression, targetFile)
      //file is written here
      copyStream(new FileInputStream(typeConvertedFile.toJava), compressedOutStream)

      try {
        typeConvertedFile.delete()
      }
      catch {
        case noSuchFileException: NoSuchFileException => ""
      }
    }
  }

  def getOutputFile(inputFile: File, outputFormat:String, outputCompression:String,src_dir: String, dest_dir: String): File ={

    val nameWithoutExtension = inputFile.nameWithoutExtension
    val name = inputFile.name
    var filepath_new = ""
    val dataIdFile = inputFile.parent / "dataid.ttl"

    if(dataIdFile.exists) {
      val dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
      filepath_new = dest_dir.concat("/")
      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
      filepath_new = filepath_new.concat(nameWithoutExtension)
    }
    else{
      // changeExtensionTo() funktioniert nicht bei noch nicht existierendem File, deswegen ausweichen über Stringmanipulation
      filepath_new = inputFile.pathAsString.replaceAll(File(src_dir).pathAsString,File(dest_dir).pathAsString.concat("/NoDataID"))
      filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
    }

    if (outputCompression.isEmpty){
      filepath_new = filepath_new.concat(".").concat(outputFormat)
    }
    else{
      filepath_new = filepath_new.concat(".").concat(outputFormat).concat(".").concat(outputCompression)
    }

    val outputFile = File(filepath_new)
    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    println(s"Converted File: ${outputFile.pathAsString}\n")

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

  def downloadFile(url: String, targetdir:String): Unit = {
    println(url)
    val filepath = targetdir.concat(url.split("http://|https://").map(_.trim).last) //filepath from url without http://
    val file = File(filepath)
    file.parent.createDirectoryIfNotExists(createParents = true)
    FileUtils.copyURLToFile(new URL(url),file.toJava)

    val dataIdFile = file.parent / "dataid.ttl"
    if (!dataIdFile.exists()){  //if no dataid.ttl File in directory of downloaded file, then download the belongig dataid.ttl
//      println("Download Dataid.ttl")
      QueryHandler.getDataIdFile(url ,dataIdFile)
    }
  }

  def unionFiles(tempDir:String, targetFile:File)={
    //union all part files of Sansa

    val findTripleFiles = s"find $tempDir/ -name part*" !!
    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

    if( concatFiles == 0 ) FileUtils.deleteDirectory(File(tempDir).toJava)
    else System.err.println(s"[WARN] failed to merge $tempDir/*")

  }

  def unionFilesWithHeaderFile(headerTempDir:String, tempDir:String, targetFile:File)={
    //union all part files of Sansa

    val findTripleFiles = s"find $headerTempDir/ -name part*" #&& s"find $tempDir/ -name part*" !!
    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

    if( concatFiles == 0 ){
      FileUtils.deleteDirectory(File(tempDir).toJava)
      FileUtils.deleteDirectory(File(headerTempDir).toJava)
    }
    else System.err.println(s"[WARN] failed to merge $tempDir/*")

  }
}
