import java.net.URL
import sys.process._
import better.files.File


object FileHandler {

  val src_dir: String="./downloaded_files/"


  def readQuery(file:File) : String = {

    var queryString:String = ""
    for (line <- file.lineIterator) {
      queryString = queryString.concat(line).concat("\n")
    }
    return queryString
  }

  def downloadFile(url: String): Unit = {

    //filepath from url without http://
    var filepath = src_dir.concat(url.split("//").map(_.trim).last)
    var file = File(filepath)
    file.parent.createDirectoryIfNotExists(createParents = true)
    new URL(url) #> file.toJava !!
  }


  def convertFile(inputFile:File, dest_dir:String, outputFormat:String, outputCompression:String): Unit = {

    val decompressedStream = Converter.decompress(inputFile)

    //noch ohne Funktion
    val convertedStream = Converter.convertFormat(decompressedStream, outputFormat)

    val compressedFile = getOutputFile(inputFile, outputFormat, outputCompression, dest_dir)

    Converter.compress(decompressedStream, outputCompression, compressedFile)
  }


  def getOutputFile(inputFile: File, outputFormat:String, outputCompression:String, dest_dir: String): File ={

    var filepath_new = inputFile.toString().replaceAll(src_dir.substring(1),dest_dir.substring(1))
    println(filepath_new)

    // CATCH EXCEPTION WHEN FILE ALREADY EXISTS
    try{
      var outputFile = File(filepath_new).changeExtensionTo(".".concat(outputFormat).concat(".").concat(outputCompression))
      println(outputFile.toString())
      //create necessary parent directories to write the outputfile there, later
      outputFile.parent.createDirectoryIfNotExists(createParents = true)
      return outputFile
    }

  }




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
