package download

import org.apache.jena.query._
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.dbpedia.databus.client.sparql.QueryHandler
import org.dbpedia.databus.client.sparql.queries.{DataIdQueries, DatabusQueries}
import org.scalatest.flatspec.AnyFlatSpec

class QueryTest extends AnyFlatSpec {

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


  "look how empty result " should "look" in {
    val result =QueryHandler.executeQuery(DatabusQueries.queryDataId("iasd.com"))

    println(result.isEmpty)
    println(result.head.varNames())
  }

  "asd" should "asd" in {

    val str =
      """
        |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
        |PREFIX dct: <http://purl.org/dc/terms/>
        |PREFIX dcat:  <http://www.w3.org/ns/dcat#>
        |SELECT DISTINCT ?file WHERE {
        |    ?dataset dataid:artifact <https://databus.dbpedia.org/marvin/mappings/geo-coordinates-mappingbased> .
        |    ?dataset dcat:distribution ?distribution .
        |    ?distribution dcat:downloadURL ?file .
        |}
        |Limit 10 Offset 10
        |
        |""".stripMargin
    val result = QueryHandler.executeQuery(str)

    result.foreach(println(_))
    val mediaTypes = QueryHandler.getMediaTypes(result.map(querySolution => querySolution.getResource("?file").toString))


  }

//  "mappings" should "be queried" in {
//    QueryHandler.getMapping("https://databus.dbpedia.org/kurzum/mastr/bnetza-mastr/01.04.00/bnetza-mastr_rli_type=hydro.csv.bz2").foreach(println(_))
//  }

  "query" should "work for dataid" in {

    val dataIdModel: Model = RDFDataMgr.loadModel("./target/databus.tmp/cache_dir/janni/newnew/newnew/2022-05-12/dataid.jsonld", RDFLanguages.JSONLD)

    var query = s"""
                   |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
                   |PREFIX dct: <http://purl.org/dc/terms/>
                   |
                   |SELECT ?publisher ?group ?artifact ?version {
                   |  ?dataset  dct:publisher ?publisher .
                   |  ?group a dataid:Group .
                   |  ?artifact a dataid:Artifact .
                   |  ?version a dataid:Version .
       |}
    """.stripMargin

    val results = QueryHandler.executeQuery(query, dataIdModel)
    val result = results.head

    println(result)
  }

}
