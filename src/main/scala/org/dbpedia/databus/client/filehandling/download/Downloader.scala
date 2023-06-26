package org.dbpedia.databus.client.filehandling.download

import java.io.{FileNotFoundException, FileOutputStream, FileWriter, IOException}
import java.net.URL
import better.files.File
import org.apache.commons.io.IOUtils
import org.dbpedia.databus.client.Config
import org.dbpedia.databus.client.filehandling.{FileUtil, Utils}
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

object Downloader {

  /**
    * Download all files of Download-Query
    *
    * @param queryString downloadQuery
    * @param cacheDir directory for downloaded files
    * @param overwrite overwrite already downloaded files
    * @return Seq of shas of downloaded files
    */
  def downloadWithQuery(queryString: String, endpoint: String, cacheDir: File, overwrite: Boolean = false): Seq[String] = {
    println("GETTING FILE IRIS")
    val fileIRIs = QueryHandler.executeSingleVarQuery(queryString, Left(endpoint))
    var allSHAs = Seq.empty[String]

    println("--------------------------------------------------------")
    println("Files to download:")

    fileIRIs.foreach(fileIRI => {
//      val fileEndpoint = Utils.getDomainName(fileIRI).concat("/sparql")
      val fileInfoOption = QueryHandler.getFileInfo(fileIRI, endpoint)

        if (fileInfoOption.isDefined) {
          val fileInfo: DownloadConfig = fileInfoOption.get
          val outputFile = fileInfo.getOutputFile(cacheDir)

          def downloadFileWithShaLog(): Unit ={
            downloadFile(fileInfo.downloadURL, fileInfo.sha, outputFile) match {
              case Some(downloadedFile: File) =>
                allSHAs = allSHAs :+ fileInfo.sha
                //log sha of file and link to file in cache
                val fw = new FileWriter(cacheDir.pathAsString.concat("/shas.txt"), true)
                fw.append(s"${fileInfo.sha}\t${outputFile.pathAsString}\n")
                fw.close()

                //check if belonging dataid.ttl exists. If not, download it.
                val dataIdFile = downloadedFile.parent / "dataid.jsonld"
                if (!dataIdFile.exists()) {
                  QueryHandler.downloadDataIdFile(fileInfo.dataidURL, dataIdFile)
                }
              case None => ""
            }
          }

          if (overwrite) {
            downloadFileWithShaLog
          }
          else {
            if (!FileUtil.checkIfFileInCache(cacheDir, fileInfo.sha)) {
              downloadFileWithShaLog
            }
            else {
              println(s"$fileIRI --> already exists in Cache")
              allSHAs = allSHAs :+ fileInfo.sha
            }
          }
        } else {
          ""
        }
    })


    allSHAs
  }



//  def getOutputFile(iri:String):File={
//    QueryHandler.executeQuery()
//
//  }
  /**
    * Download a file and its dataID-file and record it in the cache(shas.txt)
    *
    * @param url downloadURL
    * @param sha sha of file to download
    * @param targetDir target directory
    * @return Boolean, return true if download succeeded
    */
  def downloadFile(url: String, sha: String, file: File): Option[File] = {

    val tempFile = Config.cache / (url.splitAt(url.lastIndexOf("/")+1)._2)
    var correctFileTransfer = false

    //try to download file 3 times if file transfer errors occure
    (0 to 4).iterator
      .takeWhile(_ => !correctFileTransfer)
      .foreach(_ => {
        Downloader.downloadUrlToFile(new URL(url), tempFile, createParentDirectory = true)
        try {
          correctFileTransfer = FileUtil.checkSum(tempFile, sha)
        } catch {
          case fileNotFoundException: FileNotFoundException => ""
        }
      })

    if (!correctFileTransfer) {
      println("file download had issues")
      LoggerFactory.getLogger("Download-Logger").error(s"couldn't download file $url properly")
      file.delete(swallowIOExceptions = true)
      return None
    }

    file.parent.createDirectoryIfNotExists()
    tempFile.moveTo(file, true)

    Some(file)
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

//  /**
//   * Download URL to a directory (and create subdirectories depending on slashes of url)
//   *
//   * @param url downloadURL
//   * @param directory target
//   * @param createDirectory create directory
//   * @param skipIfExists skip download if file already exists
//   */
//  def downloadUrlToDirectory(url: URL, directory: File,
//                             createDirectory: Boolean = false, skipIfExists: Boolean = false): Unit = {
//
//    val file = directory / url.getFile.split("/").last
//    if (!(skipIfExists && file.exists)) downloadUrlToFile(url, file, createDirectory)
//  }


}
