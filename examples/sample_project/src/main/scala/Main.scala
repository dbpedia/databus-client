import org.dbpedia.databus.client.api.DatabusClient
import org.dbpedia.databus.client.api.DatabusClient.{Compression, Format}

object Main {

  def main(args: Array[String]): Unit = {

    //Example of how to use
    //    DatabusClient.source("./src/query/query1").source("./src/query/query2").execute()

    val query =
      """
        |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
        |PREFIX dct: <http://purl.org/dc/terms/>
        |PREFIX dcat:  <http://www.w3.org/ns/dcat#>
        |
        |SELECT DISTINCT ?file WHERE {
        |    ?dataset dataid:artifact <https://databus.dbpedia.org/marvin/mappings/geo-coordinates-mappingbased> .
        |    ?dataset dcat:distribution ?distribution .
        |    ?distribution dcat:downloadURL ?file .
        |}
        |Limit 2 Offset 2
        |""".stripMargin

    DatabusClient
      .source(query)
      .compression(Compression.gz)
      .format(Format.nt)
      .execute()


    /*
    Continue to work with the data here...
     */
  }
}
