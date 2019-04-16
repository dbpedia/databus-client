import scala.io.Source

object FileRead {

  def fileread(file:String) : String = {

    var queryString:String = ""
    val filename = file
    for (line <- Source.fromFile(filename).getLines) {
      queryString = queryString.concat(line)
      queryString = queryString.concat("\n")
    }
    return queryString
  }
}