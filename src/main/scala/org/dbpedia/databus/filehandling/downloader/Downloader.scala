package org.dbpedia.databus.filehandling.downloader

import java.io.{FileNotFoundException, FileOutputStream}
import java.net.URL

import better.files.File
import org.apache.commons.io.IOUtils
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.sparql.QueryHandler
import org.slf4j.LoggerFactory

object Downloader {

  def downloadWithQuery(queryString: String, targetdir: File, overwrite:Boolean=false): Seq[String] = {
    val results = QueryHandler.executeDownloadQuery(queryString)
    var allSHAs = Seq.empty[String]

    println("--------------------------------------------------------\n")
    println("Files to download:")

    results.foreach(fileIRI => {
      val fileSHA = QueryHandler.getSHA256Sum(fileIRI)
      if (overwrite) downloadFile(fileIRI, targetdir)
      else {
        if (!FileUtil.checkIfFileInCache(targetdir, fileSHA)) downloadFile(fileIRI, targetdir)
        else println(s"$fileIRI --> already exists in Cache")
      }
      allSHAs = allSHAs :+ fileSHA
    })

    allSHAs
  }

  def downloadFile(url: String, targetdir: File): Unit = {
    val file = targetdir / url.split("http[s]?://").map(_.trim).last //filepath from url without http://

    downloadUrlToFile(new URL(url), file, createParentDirectory = true)

    val dataIdFile = file.parent / "dataid.ttl"

    if (!dataIdFile.exists()) { //if no dataid.ttl File in directory of downloaded file, then download the belongig dataid.ttl
      try {
        QueryHandler.downloadDataIdFile(url, dataIdFile)
      }
      catch {
        case _: FileNotFoundException =>
          println("couldn't query dataidfile")
          LoggerFactory.getLogger("DataID-Logger").error("couldn't query dataidfile")

      }
    }
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

  def downloadUrlToDirectory(url: URL, directory: File,
                             createDirectory: Boolean = false, skipIfExists: Boolean = false): Unit = {

    val file = directory / url.getFile.split("/").last
    if (!(skipIfExists && file.exists)) downloadUrlToFile(url, file, createDirectory)
  }


  def readQueryFile(file: File): String = {
    var queryString: String = ""
    for (line <- file.lineIterator) {
      queryString = queryString.concat(line).concat("\n")
    }
    queryString
  }
}
