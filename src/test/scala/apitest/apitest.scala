package apitest

import org.dbpedia.databus.client.api.DatabusClient
import org.dbpedia.databus.client.api.DatabusClient.{Compression, Format}
import org.scalatest.flatspec.AnyFlatSpec

class apitest extends AnyFlatSpec {

  "DatabusObject" should "execute the Databus Main_DownloadConvert function in the right way" in {
    DatabusClient
      .source("./src/query/query3")
      .compression(Compression.bz2)
      .format(Format.nt)
      .target("./test/")
      .execute()
  }

  "DatabusObject" should "execute the Databus Main_Convert function in the right way" in {
    DatabusClient
      .source("/home/eisenbahnplatte/git/databus-client/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-format.conversion-testbed_bob4.ttl")
      .compression(Compression.bz2)
      .format(Format.nt)
      .target("./test/")
      .execute()
  }

  "DatabusObject" should "just download files of query" in {
    DatabusClient
      .source("./src/query/query3")
      .execute()
  }

  "DatabusObject" should "convert files downloaded with query and already existing files, too" in {
    DatabusClient
      .source("./src/query/query3.query")
      .source("/home/eisenbahnplatte/git/databus-client/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-format.conversion-testbed_bob4.ttl")
      .format(Format.nt)
      .config("overwrite", "true")
      .compression(Compression.bz2)
      .execute()
  }

}
