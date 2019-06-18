package org.dbpedia.databus

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.net.URL

import better.files.File
import org.apache.commons.io.{FileUtils, IOUtils}

object FileHandler {

  val src_dir: String="./downloaded_files/"

  def convertFile(inputFile:File, dest_dir:String, outputFormat:String, outputCompression:String): Unit = {
    val bufferedInputStream = new BufferedInputStream(new FileInputStream(inputFile.toJava))

    if (outputCompression=="same" && outputFormat=="same"){
      val compressionInputFile = Converter.getCompressionType(bufferedInputStream)
      val formatInputFile = Converter.getFormatType(Converter.decompress(bufferedInputStream))
      val outputStream = new FileOutputStream(getOutputFile(inputFile, formatInputFile, compressionInputFile, dest_dir).toJava)
      copyStream(new FileInputStream(inputFile.toJava), outputStream)
    }
    else if (outputCompression=="same" && outputFormat!="same"){
      val compressionInputFile= Converter.getCompressionType(bufferedInputStream)
      val decompressedInStream = Converter.decompress(bufferedInputStream)

      //noch ohne Funktion
      val convertedStream = Converter.convertFormat(decompressedInStream, outputFormat)

      val compressedFile = getOutputFile(inputFile, outputFormat, compressionInputFile, dest_dir)
      val compressedOutStream = Converter.compress(compressionInputFile, compressedFile)

      //file is written here
      copyStream(decompressedInStream, compressedOutStream)
    }
    else if (outputCompression!="same" && outputFormat=="same"){
      val decompressedInStream = Converter.decompress(bufferedInputStream)
      val format = Converter.getFormatType(decompressedInStream)
      val compressedFile = getOutputFile(inputFile, format, outputCompression, dest_dir)
      val compressedOutStream = Converter.compress(outputCompression, compressedFile)

      //file is written here
      copyStream(decompressedInStream, compressedOutStream)
    }
    else{
      val decompressedInStream = Converter.decompress(bufferedInputStream)
      //noch ohne Funktion
      val convertedStream = Converter.convertFormat(decompressedInStream, outputFormat)

      val compressedFile = getOutputFile(inputFile, outputFormat, outputCompression, dest_dir)
      val compressedOutStream = Converter.compress(outputCompression, compressedFile)

      //file is written here
      copyStream(decompressedInStream, compressedOutStream)
    }
  }

  def getOutputFile(inputFile: File, outputFormat:String, outputCompression:String, dest_dir: String): File ={


    val nameWithoutExtension = inputFile.nameWithoutExtension
    val name = inputFile.name
    var filepath_new:String = ""
    val dataIdFile = inputFile.parent / "dataid.ttl"

    if(dataIdFile.exists) {
      var dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
      filepath_new = dest_dir.concat("/")
      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
      filepath_new = filepath_new.concat(nameWithoutExtension)
    }
    else{
      filepath_new = inputFile.pathAsString.replaceAll(File(src_dir).pathAsString,File(dest_dir).pathAsString.concat("/NoDataID"))
      // changeExtensionTo() funktioniert nicht, deswegen ausweichen über Stringmanipulation
      filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
    }

    if (outputCompression.isEmpty){
      filepath_new = filepath_new.concat(".").concat(outputFormat)
    }
    else{
      filepath_new = filepath_new.concat(".").concat(outputFormat).concat(".").concat(outputCompression)
    }

    var outputFile = File(filepath_new)
    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)
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
    if (!(file.parent / "dataid.ttl").exists()){
      println("gibt keine Dataid.ttl")
      downloadDataIdFile(url)
    }
  }

  def downloadDataIdFile(url: String)={
    var dataIdURL = url.substring(0,url.lastIndexOf("/")).concat("/dataid.ttl")
    var filepath = src_dir.concat(dataIdURL.split("http://|https://").map(_.trim).last)
    var file = File(filepath)
    FileUtils.copyURLToFile(new URL(dataIdURL),file.toJava)
  }
}

//    val newFileName = s"${inputFile.nameWithoutExtension(false)}.$outputFormat.$outputCompression"


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

