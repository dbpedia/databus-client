package org.dbpedia.databus.client.sparql.queries

object DataIdQueries {

  def queryDirStructure(): String = {
    s"""
       |PREFIX databus: <https://dataid.dbpedia.org/databus#>
       |PREFIX dct: <http://purl.org/dc/terms/>
       |
       |SELECT ?publisher ?group ?artifact ?version {
       |  ?dataset  dct:publisher ?publisher .
       |  ?group a databus:Group .
       |  ?artifact a databus:Artifact .
       |  ?version a databus:Version .
       |}
    """.stripMargin
  }

  def queryFileExtension(fileName: String): String = {
    s"""
       |PREFIX databus: <https://dataid.dbpedia.org/databus#>
       |SELECT ?type {
       |  ?distribution databus:formatExtension ?type .
       |  FILTER regex(str(?distribution), "#$fileName") .
       |}
    """.stripMargin
  }

}
