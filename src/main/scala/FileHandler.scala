import java.net.URL
import java.io.File
import sys.process._

import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.io.Source


object FileHandler {

  val src_dir: String="./downloaded_files/"


  def readQuery(filepath:String) : String = {

    var queryString:String = ""
    for (line <- Source.fromFile(filepath).getLines) {
      queryString = queryString.concat(line)
      queryString = queryString.concat("\n")
    }
    return queryString
  }

  def downloadFile(url: String): Unit = {

    var filename = url.split("/").map(_.trim).last
    println(filename)
    filename = src_dir.concat(filename)
    new URL(url) #> new File(filename) !!
  }


  def convertFile(filepath:String, dest_dir:String): Unit = {
    var extension:String = null

    if (filepath.contains(".")) {
      extension = filepath.substring(filepath.lastIndexOf(".") + 1)
    }

    //if file already in gzip format, just copy to the destination dir
    if (extension=="gzip"){
      moveFile(filepath, dest_dir)
    }
    else{
      var inputFile:File = new File(filepath)

      var filepath_new = filepath.substring(filepath.lastIndexOf("/") + 1)
      filepath_new = dest_dir.concat(filepath_new.substring(0,filepath_new.lastIndexOf(".")))
      var outputFile:File = new File(filepath_new)
      println(filepath)

      ConvertCompression.decompress(inputFile,outputFile)

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
