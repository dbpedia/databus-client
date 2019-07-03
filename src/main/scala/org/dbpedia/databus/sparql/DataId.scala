package org.dbpedia.databus.sparql

object DataIdQueries {

  def queryGetPublisher(): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?o {
       |  ?dataset a dataid:Dataset;
       |           dataid:account ?o .
       |}
    """.stripMargin

  def queryGetGroup(): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?o {
       |  ?dataset a dataid:Dataset;
       |           dataid:group ?o .
       |}
    """.stripMargin

  def queryGetArtifact(): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?o {
       |  ?dataset a dataid:Dataset;
       |           dataid:artifact ?o .
       |}
    """.stripMargin

  def queryGetVersion(): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?o {
       |  ?dataset a dataid:Dataset;
       |           dataid:version ?o .
       |}
    """.stripMargin

  def queryGetType(fileURL:String): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?type {
       |  <$fileURL> dataid:formatExtension ?type .
       |}
    """.stripMargin
}
