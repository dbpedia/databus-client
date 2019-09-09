package org.dbpedia.databus.cli

import org.rogach.scallop._

class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""

Example Download and Convert: mvn scala:run -Dlauncher=downloadconverter -q ./src/query/downloadquery --targetrepo converted_files/ -c gz -f jsonld
Example Download only: mvn scala:run -Dlauncher=downloader -q ./src/query/downloadquery -t ./downloaded_files/
Example Convert only: mvn scala:run -Dlauncher=converter --src ./downloaded_files/ -t ./converted_files/ -c gz -f jsonld

For usage see below:
    """)

  val query = opt[String]("query", default= Some("./src/query/downloadquery"), descr = "any ?file query; You can pass the query directly or save it in a textfile and pass the filepath")
  val destination_dir = opt[String]("destination", default= Some("./files/"), descr = "set the destination directory for converted files")
  val source_dir = opt[String]("source", default= Some("./temp_downloaded_files/"), descr = "set the source directory for files you want to convert")
  val output_format = opt[String]("format", default= Some("same"), descr = "set the file format of the output file")
  val output_compression = opt[String]("compression", default= Some(""), descr = "set the compression format of the output file")
  val help = opt[Boolean]("help", noshort = true, descr = "show this message")

  verify()

}



