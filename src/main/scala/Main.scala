import better.files.File
import org.rogach.scallop._

object Main {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)


    var file = File("/home")
    val du = file / "fabian"

    //Test if query is a File or a Query
    var queryString:String = ""
    File(conf.query()).exists() match {
      case true => {
        // "./src/query/query"
        val file = File(conf.query())
        queryString = FileHandler.readQuery(file)
      }
      case false => {
        queryString = conf.query()
      }
    }


   // val dest_dir:String="./converted_files/"
    println(queryString)

    SelectQuery.execute(queryString)

    file = File("./downloaded_files/geo-coordinates-mappingbased_lang=be.ttl.bz2")
    FileHandler.convertFile("./downloaded_files/geo-coordinates-mappingbased_lang=be.ttl.bz2", conf.localrepo(),file)

  }

}