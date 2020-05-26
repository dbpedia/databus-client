package org.dbpedia.databus.client.filehandling.download

import java.io.{FileNotFoundException, FileOutputStream, FileWriter, IOException}
import java.net.URL

import better.files.File
import org.apache.commons.io.IOUtils
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

object Downloader {

  /**
    * Download all files of Download-Query
    *
    * @param queryString downloadQuery
    * @param targetdir directory for downloaded files
    * @param overwrite overwrite already downloaded files
    * @return Seq of shas of downloaded files
    */
  def downloadWithQuery(queryString: String, targetdir: File, overwrite: Boolean = false): Seq[String] = {
    val results = QueryHandler.executeDownloadQuery(queryString)
    var allSHAs = Seq.empty[String]

    println("--------------------------------------------------------")
    println("Files to download:")

    results.foreach(fileIRI => {
      val fileSHA = QueryHandler.getSHA256Sum(fileIRI)

      if (overwrite) {
        downloadFile(fileIRI, fileSHA, targetdir) match {
          case Some(file: File) => allSHAs = allSHAs :+ fileSHA
          case None => ""
        }
      }
      else {
        if (!FileUtil.checkIfFileInCache(targetdir, fileSHA)) {
          downloadFile(fileIRI, fileSHA, targetdir) match {
              case Some(file: File) => allSHAs = allSHAs :+ fileSHA
              case None => ""
          }
        }
        else {
          println(s"$fileIRI --> already exists in Cache")
          allSHAs = allSHAs :+ fileSHA
        }
      }
    })

    allSHAs
  }

  /**
    * Download a file and its dataID-file and record it in the cache(shas.txt)
    *
    * @param url downloadURL
    * @param sha sha of file to download
    * @param targetDir target directory
    * @return Boolean, return true if download succeeded
    */
  def downloadFile(url: String, sha: String, targetDir: File): Option[File] = {
    val file = targetDir / url.split("http[s]?://").map(_.trim).last //filepath from url without http://

    var correctFileTransfer = false

    //repeat download 5 times if file transfer errors occure
    (0 to 4).iterator
      .takeWhile(_ => !correctFileTransfer)
      .foreach(_ => {
        Downloader.downloadUrlToFile(new URL(url), file, createParentDirectory = true)
        correctFileTransfer = FileUtil.checkSum(file, sha)
      })

    if (!correctFileTransfer) {
      println("file download had issues")
      LoggerFactory.getLogger("Download-Logger").error(s"couldn't download file $url properly")
      file.delete()
      return None
    }

    val fw = new FileWriter(targetDir.pathAsString.concat("/shas.txt"), true)
    fw.append(s"$sha\t${file.pathAsString}\n")
    fw.close()


    val dataIdFile = file.parent / "dataid.ttl"

    if (!dataIdFile.exists()) { //if no dataid.ttl File in directory of downloaded file, then download the belongig dataid.ttl
      if(!QueryHandler.downloadDataIdFile(url, dataIdFile)) {
        println("couldn't query dataidfile")
        LoggerFactory.getLogger("DataID-Logger").error("couldn't query dataidfile")
      }
    }

    Some(file)
  }

  /**
    * Download URL to a directory (and create subdirectories depending on slashes of url)
    *
    * @param url downloadURL
    * @param directory target
    * @param createDirectory create directory
    * @param skipIfExists skip download if file already exists
    */
  def downloadUrlToDirectory(url: URL, directory: File,
                             createDirectory: Boolean = false, skipIfExists: Boolean = false): Unit = {

    val file = directory / url.getFile.split("/").last
    if (!(skipIfExists && file.exists)) downloadUrlToFile(url, file, createDirectory)
  }

  /**
    * Download URL to a file
    *
    * @param url downloadURL
    * @param file target file
    * @param createParentDirectory create parent directories
    */
  def downloadUrlToFile(url: URL, file: File, createParentDirectory: Boolean = false): Unit = {

    if (createParentDirectory) file.parent.createDirectoryIfNotExists()

    System.err.println(s"$url -> $file")

    try{
      val conn = url.openConnection()
      val cis = new LoggingInputStream(conn.getInputStream, conn.getContentLengthLong, 1L << 21)
      val fos = new FileOutputStream(file.toJava)

      try {
        IOUtils.copy(cis, fos)
      } finally {
        fos.close()
        cis.close()
      }
    } catch {
      case noPermission: IOException => LoggerFactory.getLogger("DownloadLogger").error(s"No Permission for ${url.toString}")
      case noInputStream: FileNotFoundException => LoggerFactory.getLogger("DownloadLogger").error(s"Uri ${url.toString} doesnt have inputstream")
    }

  }


}
