package org.dbpedia.databus.sparql.queries

object DataIdQueries {

  def queryDirStructure(): String = {
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |
       |SELECT ?publisher ?group ?artifact ?version {
       |  ?dataset a dataid:Dataset ;
       |           dataid:account ?publisher ;
       |           dataid:group ?group ;
       |           dataid:artifact ?artifact ;
       |           dataid:version ?version .
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
