import org.apache.jena.query._

class QueryTest extends org.scalatest.FlatSpec {

  "" should "" in {
    def executeDownloadQuery(queryString: String): Seq[String] = {
      val query: Query = QueryFactory.create(queryString)

      val qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)
      println(s"QUERY:\n$query")

      var filesSeq: Seq[String] = Seq[String]()
      try {
        val results: ResultSet = qexec.execSelect
        while (results.hasNext) {
          val resource = results.next().getResource("?file")
          filesSeq = filesSeq :+ resource.toString
        }
      } finally qexec.close()

      filesSeq
    }

    val queryStr =
      """
        |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
        |PREFIX dct: <http://purl.org/dc/terms/>
        |PREFIX dcat:  <http://www.w3.org/ns/dcat#>
        |SELECT DISTINCT ?file WHERE {
        |    ?dataset dataid:artifact <https://databus.dbpedia.org/marvin/mappings/geo-coordinates-mappingbased> .
        |    ?dataset dcat:distribution ?distribution .
        |    ?distribution dcat:downloadURL ?file .
        |}
        |Limit 2 Offset 2
      """.stripMargin

    val results = executeDownloadQuery(queryStr)

    results.foreach(downloadIRI => println(downloadIRI))
  }

}
