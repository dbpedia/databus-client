package org.dbpedia.databus.client.filehandling

object Utils {

  def urlOneUp(url:String):String={
    url.splitAt(url.lastIndexOf("[/#]")+1)._1
  }

  def urlTakeLast(url:String):String={
    url.splitAt(url.lastIndexOf("[/#]")+1)._2
  }

}
