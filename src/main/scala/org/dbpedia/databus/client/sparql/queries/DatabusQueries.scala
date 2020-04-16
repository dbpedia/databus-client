package org.dbpedia.databus.client.sparql.queries

object DatabusQueries {

  def querySha256 (url: String): String =
    s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |PREFIX dcat:   <http://www.w3.org/ns/dcat#>
       |
       |SELECT ?sha256
       |WHERE {
       |  ?s dcat:downloadURL <$url>  .
       |  ?s dataid:sha256sum ?sha256 .
       |}
       """.stripMargin

  def queryDataId (url: String): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |PREFIX dcat: <http://www.w3.org/ns/dcat#>
       |
       |SELECT DISTINCT ?dataset
       |WHERE {
       |  ?dataset dataid:version ?version .
       |  ?dataset dcat:distribution ?distribution .
       |  ?distribution dcat:downloadURL <$url>
       |}
       """.stripMargin

  def queryMediaType (files: String): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |PREFIX dcat: <http://www.w3.org/ns/dcat#>
       |
       |SELECT DISTINCT ?type
       |WHERE {
       |  ?distribution dcat:mediaType ?type .
       |  ?distribution dcat:downloadURL ?du .
       |FILTER (?du in (<$files>))
       |}
       |GROUP BY ?type
       |""".stripMargin

  def queryMappingInfoFile(sha: String): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |PREFIX dcat: <http://www.w3.org/ns/dcat#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |
       |SELECT DISTINCT ?mapping
       |WHERE {
       |  ?dataIdElement dataid:sha256sum "$sha"^^xsd:string .
       |  ?dataIdElement dataid:file ?file .
       |  ?mapping <http://tmp-namespace.org/databusFixRequired> ?file .
       |}
       |""".stripMargin
}
