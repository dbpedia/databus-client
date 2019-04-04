import scala.io.Source

object FileRead {

  def fileread(file:String) : Int = {

    val filename = file
    for (line <- Source.fromFile(filename).getLines) {
      println(line)
    }
    return 0
  }
}