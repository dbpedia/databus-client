package org.dbpedia.databus.filehandling

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.file.Files
import java.security.MessageDigest

import better.files.File
import org.apache.commons.io.IOUtils
import org.dbpedia.databus.sparql.QueryHandler

import scala.sys.process._

object FileUtil {

  def unionFiles(dir: File, targetFile: File): Unit = {
    //union all part files of Apache Spark
    val findTripleFiles = s"find ${dir.pathAsString}/ -name part* -not -empty" !!
    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

    if (!(concatFiles == 0)) {
      System.err.println(s"[WARN] failed to merge ${dir.pathAsString}/*")
    }
  }

  def copyUnchangedFile(inputFile: File, src_dir: File, dest_dir: File): Unit = {
    val name = inputFile.name

    val dataIdFile = inputFile.parent / "dataid.ttl"

    val outputFile = {
      if (dataIdFile.exists) QueryHandler.getTargetDir(dataIdFile, dest_dir) / name
      else File(inputFile.pathAsString.replaceAll(src_dir.pathAsString, dest_dir.pathAsString.concat("/NoDataID/")))
    }

    //    println(s"Copy unchanged File to: ${outputFile.pathAsString}")
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    val outputStream = new FileOutputStream(outputFile.toJava)
    copyStream(new FileInputStream(inputFile.toJava), outputStream)
  }

  def copyStream(in: InputStream, out: OutputStream): Unit = {
    try {
      IOUtils.copy(in, out)
    }
    finally if (out != null) {
      out.close()
    }
    //    val bytes = new Array[Byte](1024) //1024 bytes - Buffer size
    //    Iterator
    //      .continually (in.read(bytes))
    //      .takeWhile (-1 !=)
    //      .foreach (read=>out.write(bytes,0,read))
    //    out.close()
  }

  def checkIfFileInCache(cache_dir: File, fileSHA256: String): Boolean = {
    var exists = false

    val files = cache_dir.listRecursively.toSeq
    for (file <- files) {
      if (!file.isDirectory) {
        if (!file.name.equals("dataid.ttl")) {
          if (checkSum(file, fileSHA256)) {
            exists = true
          }
        }
      }
    }

    exists
  }

  def getSha256(file:File) : String = {
    MessageDigest.getInstance("SHA-256")
      .digest(Files.readAllBytes(file.path))
      .map("%02x".format(_)).mkString
  }

  def checkSum(file: File, sha: String): Boolean = {
    if (getSha256(file)== sha) true
    else false
  }

  def getFileWithSHA256(sha: String, dir: File): File = {
    var fileOfSha = File("")

    val files = dir.listRecursively.toSeq
    for (file <- files) {
      if (!file.isDirectory) {
        if (!file.name.equals("dataid.ttl")) {
          if (checkSum(file, sha)) {
            fileOfSha = file
          }
        }
      }
    }

    fileOfSha
  }
}


//  def unionFilesWithHeaderFile(headerTempDir: File, tempDir: File, targetFile: File, deleteTemp:Boolean = true): Unit = {
//    //union all part files of Apache Spark
//
//    val findTripleFiles = s"find ${headerTempDir.pathAsString}/ -name part*" #&& s"find ${tempDir.pathAsString}/ -name part*" !!
//    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !
//
//    if (concatFiles == 0) {
//      if (deleteTemp) {
//        headerTempDir.delete()
//        tempDir.delete()
//      }
//    }
//    else System.err.println(s"[WARN] failed to merge ${tempDir.pathAsString}/*")
//  }