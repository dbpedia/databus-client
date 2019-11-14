package org.dbpedia.databus.filehandling

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.file.Files
import java.security.MessageDigest

import better.files.File
import org.apache.commons.io.{FileUtils, IOUtils}
import org.dbpedia.databus.sparql.QueryHandler

import scala.sys.process._

object FileUtil {

  def copyStream(in: InputStream, out: OutputStream): Unit = {
    try {
      IOUtils.copy(in, out)
    }
    finally if (out != null) {
      out.close()
    }
  }

  def unionFiles(tempDir: File, targetFile: File): Unit = {
    //union all part files of Apache Spark
    val findTripleFiles = s"find ${tempDir.pathAsString}/ -name part* -not -empty" !!
    val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

    if (!(concatFiles == 0)) {
      System.err.println(s"[WARN] failed to merge ${tempDir.pathAsString}/*")
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

  def copyUnchangedFile(inputFile: File, src_dir: File, dest_dir: File): Unit = {
    val name = inputFile.name

    println("MOINSEN")
    val dataIdFile = inputFile.parent / "dataid.ttl"

//    var filepath_new = ""
//    if (dataIdFile.exists) {
//      val dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
//      filepath_new = dest_dir.pathAsString.concat("/")
//      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
//      filepath_new = filepath_new.concat(name)
//    }
//    else {
//      filepath_new = inputFile.pathAsString.replaceAll(src_dir.pathAsString, dest_dir.pathAsString.concat("/NoDataID/"))
//    }

    val outputFile = {
      if (dataIdFile.exists)  QueryHandler.getTargetDir(dataIdFile, dest_dir) / name
      else  File(inputFile.pathAsString.replaceAll(src_dir.pathAsString, dest_dir.pathAsString.concat("/NoDataID/")))
    }

    println(outputFile.pathAsString)
    //    val outputFile = File(filepath_new)

    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    val outputStream = new FileOutputStream(outputFile.toJava)
    copyStream(new FileInputStream(inputFile.toJava), outputStream)
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

  def checkSum(file: File, sha: String): Boolean = {
    var check = false
    if (MessageDigest.getInstance("SHA-256")
      .digest(Files.readAllBytes(file.path))
      .map("%02x".format(_)).mkString == sha) check = true

    check
  }
}
