package org.dbpedia.databus.client.filehandling

import java.net.URI
import java.net.URISyntaxException
object Utils {

  def urlOneUp(url:String):String={
    url.splitAt(url.lastIndexOf("[/#]")+1)._1
  }

  def urlTakeLast(url:String):String={
    url.splitAt(url.lastIndexOf("[/#]")+1)._2
  }


  @throws(classOf[URISyntaxException])
  def getDomainName(url: String): String = {
    val uri = new URI(url)
    val domain = uri.getHost
    val protocol = uri.getScheme
    if (domain.startsWith("www.")) return protocol+"://"+domain.substring(4)
    else return protocol+"://"+domain
  }
}
