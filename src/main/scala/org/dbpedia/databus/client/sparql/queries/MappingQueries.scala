package org.dbpedia.databus.client.sparql.queries

object MappingQueries {

  def queryMappingFile(mappingInfoFile:String): String =
    s"""
       |PREFIX map: <http://databus-client.tools.dbpedia.org/vocab/mapping/>
       |
       |SELECT DISTINCT ?format.mapping
       |WHERE {
       |?format.mapping a map:MappingFile .
       |<$mappingInfoFile> map:hasMappingFile ?format.mapping .
       |}
       |""".stripMargin

  def queryMappingFileAndInfo(mappingInfoFile:String): String =
    s"""
       |PREFIX map: <http://databus-client.tools.dbpedia.org/vocab/mapping/>
       |
       |SELECT DISTINCT *
       |WHERE {
       |?format.mapping a map:MappingFile .
       |
       |<$mappingInfoFile> map:hasDelimiter ?delimiter ;
       |	  map:hasQuotation ?quotation ;
       |    map:hasMappingFile ?format.mapping .
       |}
       |""".stripMargin
}
