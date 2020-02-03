package org.dbpedia.databus.filehandling.download

import java.io.{FileNotFoundException, FileOutputStream, FileWriter}
import java.net.URL

import better.files.File
import org.apache.commons.io.IOUtils
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.sparql.QueryHandler
import org.slf4j.LoggerFactory

object Downloader {

  def downloadWithQuery(queryString: String, targetdir: File, overwrite: Boolean = false): Seq[String] = {
    val results = QueryHandler.executeDownloadQuery(queryString)
    var allSHAs = Seq.empty[String]

    println("--------------------------------------------------------")
    println("Files to download:")

    results.foreach(fileIRI => {
      val fileSHA = QueryHandler.getSHA256Sum(fileIRI)
      if (overwrite) downloadFile(fileIRI, fileSHA, targetdir)
      else {
        if (!FileUtil.checkIfFileInCache(targetdir, fileSHA)) downloadFile(fileIRI, fileSHA, targetdir)
        else println(s"$fileIRI --> already exists in Cache")
      }
      allSHAs = allSHAs :+ fileSHA
    })

    allSHAs
  }

  def downloadFile(url: String, sha: String, targetDir: File): Unit = {
    val file = targetDir / url.split("http[s]?://").map(_.trim).last //filepath from url without http://

    downloadUrlToFile(new URL(url), file, createParentDirectory = true)

    val fw = new FileWriter(targetDir.pathAsString.concat("/shas.txt"), true)
    try {
      fw.append(s"$sha\t${file.pathAsString}\n")
    }
    finally fw.close()


    val dataIdFile = file.parent / "dataid.ttl"

    if (!dataIdFile.exists()) { //if no dataid.ttl File in directory of downloaded file, then download the belongig dataid.ttl
//      try {
            if(!QueryHandler.downloadDataIdFile(url, dataIdFile)) {
              println("couldn't query dataidfile")
              LoggerFactory.getLogger("DataID-Logger").error("couldn't query dataidfile")
            }
//      }
//      catch {
//        case _: FileNotFoundException =>
//          println("couldn't query dataidfile")
//          LoggerFactory.getLogger("DataID-Logger").error("couldn't query dataidfile")
//
//      }
    }
  }

  def downloadUrlToDirectory(url: URL, directory: File,
                             createDirectory: Boolean = false, skipIfExists: Boolean = false): Unit = {

    val file = directory / url.getFile.split("/").last
    if (!(skipIfExists && file.exists)) downloadUrlToFile(url, file, createDirectory)
  }

  def downloadUrlToFile(url: URL, file: File, createParentDirectory: Boolean = false): Unit = {

    if (createParentDirectory) file.parent.createDirectoryIfNotExists()

    System.err.println(s"$url -> $file")

    val conn = url.openConnection()
    val cis = new LoggingInputStream(conn.getInputStream, conn.getContentLengthLong, 1L << 21)
    val fos = new FileOutputStream(file.toJava)

    try {
      IOUtils.copy(cis, fos)
    } finally {
      fos.close()
      cis.close()
    }
  }

}
