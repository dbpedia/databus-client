import better.files.File
import org.rogach.scallop._

object Main {

  def main(args: Array[String]) {

    val conf = new CLIConf(args)


    //Test if query is a File or a Query
    var queryString:String = ""
    File(conf.query()).exists() match {
      case true => {
        // "./src/query/query"
        queryString = FileHandler.readQuery(conf.query())
      }
      case false => {
        queryString = conf.query()
      }
    }


   // val dest_dir:String="./converted_files/"
    println(queryString)

    SelectQuery.execute(queryString)

    FileHandler.convertFile("./downloaded_files/geo-coordinates-mappingbased_lang=be.ttl.bz2", conf.localrepo())

  }

}