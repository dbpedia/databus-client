package downloaderTests

import java.net.URL

import better.files.File
import org.dbpedia.databus.client.filehandling.download.Downloader
import org.scalatest.FlatSpec

class downloaderTest extends FlatSpec{


  "urls" should "be split right" in {

    val url1 = "http://dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.ttl.bz2"
    val url2 = "https://dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=cs.ttl.bz2"

    val split = url1.split("http[s]?://").last
    println(split)
    url1.split("http://|https://").map(_.trim).last
  }

  "downloader" should "not interupt when getting bad uri" in {
    val url = "http://downloads.dbpedia.org/repo/dbpedia/spotlight/spotlight-wikistats/2020.03.11/spotlight-wikistats_type=sfAndTotalCounts_lang=zh.tsv.bz2"

    Downloader.downloadUrlToFile(new URL(url), File("./test/"))
  }
}
