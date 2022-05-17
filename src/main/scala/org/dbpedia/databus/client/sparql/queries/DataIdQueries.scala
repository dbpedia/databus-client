package org.dbpedia.databus.client.sparql.queries

object DataIdQueries {

  def queryDirStructure(): String = {
    s"""
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
  }

  def queryFileExtension(fileURL: String): String = {
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?type {
       |  <$fileURL> dataid:formatExtension ?type .
       |}
    """.stripMargin
  }

}
