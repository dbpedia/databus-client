package org.dbpedia.databus.client.sparql.queries

object MappingQueries {

  def queryMappingFile(mappingInfoFile:String): String =
    s"""
       |PREFIX map: <http://databus-client.tools.dbpedia.org/vocab/mapping/>
       |
       |SELECT DISTINCT ?mapping
       |WHERE {
       |?mapping a map:MappingFile .
       |<$mappingInfoFile> map:hasMappingFile ?mapping .
       |}
       |""".stripMargin

  def queryMappingFileAndInfo(mappingInfoFile:String): String =
    s"""
       |PREFIX map: <http://databus-client.tools.dbpedia.org/vocab/mapping/>
       |
       |SELECT DISTINCT *
       |WHERE {
       |?mapping a map:MappingFile .
       |
       |<$mappingInfoFile> map:hasDelimiter ?delimiter ;
       |	  map:hasQuotation ?quotation ;
       |    map:hasMappingFile ?mapping .
       |}
       |""".stripMargin
}
