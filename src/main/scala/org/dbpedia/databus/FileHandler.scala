package org.dbpedia.databus

import java.net.URL

import better.files.File
import org.apache.commons.io.FileUtils

object FileHandler {

  val src_dir: String="./downloaded_files/"


  def readQueryFile(file:File) : String = {

    var queryString:String = ""
    for (line <- file.lineIterator) {
      queryString = queryString.concat(line).concat("\n")
    }
    return queryString
  }

  def downloadFile(url: String): Unit = {
    println(url)
    //filepath from url without http://
    var filepath = src_dir.concat(url.split("http://|https://").map(_.trim).last)
    var file = File(filepath)

    file.parent.createDirectoryIfNotExists(createParents = true)

    /*new URL(url) #> file.toJava !!*/
    FileUtils.copyURLToFile(new URL(url),file.toJava)

    //if no dataid.ttl File in directory of downloaded file, then download the belongig dataid.ttl
    if (!file.parent.contains(file.parent/"dataid.ttl")){
        println("gibt keine Dataid.ttl")
        downloadDataID(url)
      }
    }

  def downloadDataID(url: String)={
    var dataIdURL = url.substring(0,url.lastIndexOf("/")).concat("/dataid.ttl")
    var filepath = src_dir.concat(dataIdURL.split("http://|https://").map(_.trim).last)
    var file = File(filepath)
    FileUtils.copyURLToFile(new URL(dataIdURL),file.toJava)
  }

  def convertFile(inputFile:File, dest_dir:String, outputFormat:String, outputCompression:String): Unit = {

    val decompressedStream = Converter.decompress(inputFile)
    //noch ohne Funktion
    val convertedStream = Converter.convertFormat(decompressedStream, outputFormat)
    val compressedFile = getOutputFile(inputFile, outputFormat, outputCompression, dest_dir)

    Converter.compress(decompressedStream, outputCompression, compressedFile)
  }

  def getOutputFile(inputFile: File, outputFormat:String, outputCompression:String, dest_dir: String): File ={

    if (inputFile.parent.contains(File("dataid.ttl"))){
      println("MOINI")
    }

    var filepath_new = inputFile.pathAsString.replaceAll(File(src_dir).pathAsString,File(dest_dir).pathAsString)
    val nameWithoutExtension = File(filepath_new).nameWithoutExtension
    val name = File(filepath_new).name

    // changeExtensionTo() funktioniert nicht, deswegen ausweichen über Stringmanipulation

    filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
    filepath_new = filepath_new.concat(".").concat(outputFormat).concat(".").concat(outputCompression)


    var outputFile = File(filepath_new)


    //    val newFileName = s"${inputFile.nameWithoutExtension(false)}.$outputFormat.$outputCompression"


    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)


    return outputFile

  }

  //  def getOutputFile(inputFile: File, outputFormat:String, outputCompression:String, dest_dir: String): File ={
  //
  //    var filepath_new = inputFile.pathAsString.replaceAll(File(src_dir).pathAsString,File(dest_dir).pathAsString)
  //
  //    val nameWithoutExtension = File(filepath_new).nameWithoutExtension
  //    val name = File(filepath_new).name
  //
  //    // changeExtensionTo() funktioniert nicht, deswegen ausweichen über Stringmanipulation
  //
  //    filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
  //    filepath_new = filepath_new.concat(".").concat(outputFormat).concat(".").concat(outputCompression)
  //
  //
  //    var outputFile = File(filepath_new)
  //
  //    //create necessary parent directories to write the outputfile there, later
  ////    val newFileName = s"${inputFile.nameWithoutExtension(false)}.$outputFormat.$outputCompression"
  //
  //    outputFile.parent.createDirectoryIfNotExists(createParents = true)
  //
  //
  //    return outputFile
  //
  //  }

  /*  def convertFileCompression(inputFile:File, dest_dir:String): Unit = {

      val fileType = Converter.getCompressionType(inputFile)
      var outputFile = getOutputFileForDecompress(inputFile)

      //if file already in gzip format, just copy to the destination dir
      if (fileType=="gzip"){
        if (outputFile.exists){
          inputFile.delete()
        } else {
          inputFile.moveTo(outputFile, overwrite = false)
        }
      }
      else{
        //Converter.decompress(inputFile,outputFile)
      }
    }*/

  /*def moveFile(sourceFilename:String, dest_dir:String): Unit = {

    var filename = sourceFilename.substring(sourceFilename.lastIndexOf("/") + 1)
    var destinationFilename = dest_dir.concat(filename)

    val path = Files.copy(
      Paths.get(sourceFilename),
      Paths.get(destinationFilename),
      StandardCopyOption.REPLACE_EXISTING
    )

    if (path != null) {
      println(s"copy the file $sourceFilename successfully")
    } else {
      println(s"could NOT copy the file $sourceFilename")
    }
  }*/
}
