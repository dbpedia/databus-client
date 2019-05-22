import org.rogach.scallop._



class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""

Example: scala main.scala -q "/dbpediaclient/src/query/downloadquery  --repo "/dbpediaclient/converted_files/" -o "GZIP2"

For usage see below:
    """)

  val query = opt[String]("query", required = true)
  val localrepo = opt[String]("localrepo", default= Some("./converted_files/"), descr = "set the destination dir for converted files")
  val outputFormat = opt[String]("outFormat", default= Some("ttl"), descr = "set the Fileformat you want the Output File to have")
  val outputCompression = opt[String]("outCompression", default= Some("bz2"), descr = "set the Compression you want the Output File to have")
  val help = opt[Boolean]("help", noshort = true, descr = "Show this message")

  verify()

}



