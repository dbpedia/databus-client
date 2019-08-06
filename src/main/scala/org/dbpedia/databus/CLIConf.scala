package org.dbpedia.databus

import org.rogach.scallop._

class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""

Example Download and Convert: mvn scala:run -Dlauncher=downloadconverter -q ./src/query/downloadquery --targetrepo converted_files/ -c gz -f jsonld
Example Download only: mvn scala:run -Dlauncher=downloader -q ./src/query/downloadquery -t ./downloaded_files/
Example Convert only: mvn scala:run -Dlauncher=converter --src ./downloaded_files/ -t ./converted_files/ -c gz -f jsonld

For usage see below:
    """)

  val query = opt[String]("query", default= Some("./src/query/query"), descr = "any ?file query; You can pass the query directly or save it in a textfile and pass the filepath")
  val targetrepo = opt[String]("targetrepo", default= Some("./files/"), descr = "set the destination directory for converted files")
  val src_dir = opt[String]("src", default= Some("./tempdir_downloaded_files/"), descr = "set the source directory for files you want to convert")
  val outputFormat = opt[String]("format", default= Some("same"), descr = "set the fileformat of the outputfile")
  val outputCompression = opt[String]("compression", default= Some("same"), descr = "set the compressionformat of the outputfile")
  val help = opt[Boolean]("help", noshort = true, descr = "Show this message")

  verify()

}



