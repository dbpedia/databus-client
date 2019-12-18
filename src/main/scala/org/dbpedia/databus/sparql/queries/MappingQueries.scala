package org.dbpedia.databus.sparql.queries

object MappingQueries {

  def queryMapping(mappingInfo:String): String =
    s"""
       |PREFIX tmp: <http://tmp-namespace.org/>
       |
       |SELECT DISTINCT ?mapping
       |WHERE {
       |?mapping a tmp:MappingFile .
       |<$mappingInfo> tmp:hasMappingFile ?mapping .
       |}
       |""".stripMargin
}
