package download

import better.files.File
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.download.Downloader
import org.scalatest.flatspec.AnyFlatSpec

class DownloadTest extends AnyFlatSpec {

  val testDir: File = File("./src/test/resources/queries")
  val outDir: File = testDir.parent / "output"

  "downloader" should "download with query" in {

    val queryFile = testDir / "query1.sparql"
    val queryString = FileUtil.readQueryFile(queryFile)

    Downloader.downloadWithQuery(queryString, outDir)
  }
}
