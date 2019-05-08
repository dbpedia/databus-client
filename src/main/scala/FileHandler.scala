import java.net.URL

import sys.process._
import java.nio.file.{Files, Paths, StandardCopyOption}

import better.files.File

import scala.io.Source


object FileHandler {

  val src_dir: String="./downloaded_files/"


  def readQuery(file:File) : String = {

    var queryString:String = ""
    for (line <- Source.fromFile(file.toJava).getLines) {
      queryString = queryString.concat(line).concat("\n")
    }
    return queryString
  }

  def downloadFile(url: String): Unit = {

    var filename = url.split("/").map(_.trim).last
    println(filename)
    filename = src_dir.concat(filename)
    new URL(url) #> File(filename).toJava !!
  }


  def convertFile(filepath:String, dest_dir:String, inputFile:File): Unit = {

    val fileType = Converter.getCompressionType(inputFile)


    //if file already in gzip format, just copy to the destination dir
    if (fileType=="gzip"){
      moveFile(filepath, dest_dir)
    }
    else{
      // BESSER MACHEN
      var filepath_new = inputFile.toString().substring(inputFile.toString().lastIndexOf("/") + 1)
      filepath_new = dest_dir.concat(filepath_new.substring(0,filepath_new.lastIndexOf(".")))

      var outputFile = File(filepath_new)

      Converter.decompress(inputFile,outputFile)

    }
  }

  def moveFile(sourceFilename:String, dest_dir:String): Unit = {

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
  }


}
