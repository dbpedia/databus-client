package downloaderTests

import org.apache.http.HttpHeaders
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{BasicResponseHandler, HttpClientBuilder}
import org.scalatest.FlatSpec

class getRequestTest extends FlatSpec {

  def getQueryOfCollection(uri: String): String = {
    val client = HttpClientBuilder.create().build()

    val httpGet = new HttpGet(uri)
    httpGet.addHeader(HttpHeaders.ACCEPT, "text/sparql")

    val response = client.execute(httpGet)
    val handler: ResponseHandler[String] = new BasicResponseHandler()

    handler.handleResponse(response)
  }

  "collectionSTR" should "return Query" in {
    println(getQueryOfCollection("https://databus.dbpedia.org/jfrey/collections/id-management_links"))


  }
}
