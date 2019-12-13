package org.dbpedia.databus.sparql

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

  def queryMapping (fileURL: String): String =
    s"""
       |SELECT DISTINCT *
       |WHERE {
       | 	?mapping <http://tmp-namespace.org/databusFixRequired> <$fileURL> .
       |}
       |""".stripMargin
}
