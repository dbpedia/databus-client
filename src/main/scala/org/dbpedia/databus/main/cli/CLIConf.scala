package org.dbpedia.databus.main.cli

import org.rogach.scallop._

class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  version("Databus-Client 1.0-SNAPSHOT (c) DBpedia")
  banner(
    """
Example Download and Convert: bin/DownloadConverter -q ./src/query/downloadquery --targetrepo converted_files/ -c gz -f jsonld
Example Download only: mvn scala:run -Dlauncher=downloader -q ./src/query/downloadquery -t ./downloaded_files/
Example Convert only: mvn scala:run -Dlauncher=converter --src ./downloaded_files/ -t ./converted_files/ -c gz -f jsonld

For usage see below:
    """)

  footer("\nFor all other tricks, consult the documentation!")

  //Only used in Downloader and DownloadConverter
  val query: ScallopOption[String] = opt[String](default = Some("./src/query/query1"), descr = "any ?file query; You can pass the query directly or save it in a textfile and pass the filepath")

  //Only used in Converter
  val source: ScallopOption[String] = opt[String](default = Some("./src/resources/databus-client-testbed/format-testbed/2019.08.30/"), descr = "set the source file or directory you want to convert")


  val format: ScallopOption[String] = opt[String](default = Some("same"), descr = "set the file format of the output file")
  val compression: ScallopOption[String] = opt[String](default = Some(""), descr = "set the compression format of the output file")
  val destination: ScallopOption[String] = opt[String](default = Some("./files/"), descr = "set the destination directory for converted files")

  verify()
}



