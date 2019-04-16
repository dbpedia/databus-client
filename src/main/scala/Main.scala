object Main {

  def main(args: Array[String]): Unit = {

    var filename:String = "./src/query/query2"
    var filereader = FileRead
    var queryString:String  = filereader.fileread(filename)

    println(queryString)

    var selectQuery = SelectQuery
    selectQuery.execute(queryString)


    /*val url = "http://dbpedia.org/sparql"
    @throws(classOf[java.io.IOException])
    def get(url: String) = io.Source.fromURL(url).mkString


    try {
      val content = get(url)
      println(content)
    } catch {
      case ioe: java.io.IOException =>  // handle this
      case ste: java.net.SocketTimeoutException => // handle this
    }*/

  }

}