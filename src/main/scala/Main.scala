object Main {

  def main(args: Array[String]): Unit = {

    var filename:String = "./src/query/query"
    var filehandler = FileHandling
    var queryString:String  = filehandler.fileread(filename)

    println(queryString)

    var selectQuery = SelectQuery
    selectQuery.execute(queryString)

    filehandler.convertFile("./downloaded_files/geo-coordinates-mappingbased_lang=be.ttl.bz2")

  }

}