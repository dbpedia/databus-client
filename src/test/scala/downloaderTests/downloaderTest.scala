package downloaderTests

import org.scalatest.FlatSpec

class downloaderTest extends FlatSpec{


  "urls" should "be split right" in {

    val url1 = "http://dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.ttl.bz2"
    val url2 = "https://dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=cs.ttl.bz2"

    val split = url1.split("http[s]?://").last
    println(split)
    url1.split("http://|https://").map(_.trim).last
  }
}
