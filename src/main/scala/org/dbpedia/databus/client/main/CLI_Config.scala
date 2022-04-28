package org.dbpedia.databus.client.main

import org.rogach.scallop._

class CLI_Config(arguments: Seq[String]) extends ScallopConf(arguments) {

  version("Databus Client v2.0-beta (c) DBpedia")

  banner(
    """
==============================================
Examples:
Convert only:         bin/DatabusClient --source ./downloaded_files/ --target ./converted_files/ --compression gz --format ttl
Download only:        bin/DatabusClient -s ./src/query/query1.sparql -t ./downloaded_files/
Download and convert: bin/DatabusClient -s ./src/query/query1.sparql -t converted_files/ -c gz -f ttl

You can also execute the Databus Client with 'mvn scala:run':
mvn scala:run -e -Dlauncher="databusclient" -DaddArgs="-s|https://databus.dbpedia.org/jfrey/collections/id-management_links|-f|ttl|-c|gz"
==============================================

For usage of parameters see below:
    """
  )

  footer("\nFor all other tricks, consult the documentation!")

  val source: ScallopOption[String] = opt[String](descr = "Set the source you want to convert. A source can either be a [file|directory] to convert already existing files, or a [query file|query string|collection URI] to convert queried files. Notice that query files must have .sparql or .query as extension to be recognized.") //default = Some("./src/resources/databus-client-testbed/format-testbed/2019.08.30/"),

  val format: ScallopOption[String] = opt[String](default = Some("same"), descr = "set the file format of the output file")
  val compression: ScallopOption[String] = opt[String](default = Some("same"), descr = "set the compression format of the output file")
  val target: ScallopOption[String] = opt[String](default = Some("./files/"), descr = "set the target directory for converted files")
  val overwrite: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "true -> overwrite files in cache, false -> use cache")
  val clear: ScallopOption[Boolean] = opt[Boolean](default = Some(false), noshort= true, descr = "true -> clear Cache")
  val mapping: ScallopOption[String] = opt[String](default = Some(""), descr = "set the format.mapping file for format.conversion to different format equivalence class")
  val delimiter: ScallopOption[String] = opt[String](default = Some(","), descr = "set the delimiter (only for some formats)")
  val quotation: ScallopOption[String] = opt[String](default = Some("\""), descr = "set the quotation (only for some formats)")
  val createMapping: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Do you want to create format.mapping files for mapped sources?")
  val graphURI: ScallopOption[String] = opt[String](default = Some(""), descr = "set the graph uri for format.mapping from rdf triples to rdf quads")
  val baseURI: ScallopOption[String] = opt[String](default = Some(""), descr = "set the base URI to resolve relative URIs")

  verify()
}



