package org.dbpedia.databus.client.sparql.queries

object MappingQueries {

  def queryMappingFile(mappingInfoFile:String): String =
    s"""
       |PREFIX tmp: <http://tmp-namespace.org/>
       |
       |SELECT DISTINCT ?mapping
       |WHERE {
       |?mapping a tmp:MappingFile .
       |<$mappingInfoFile> tmp:hasMappingFile ?mapping .
       |}
       |""".stripMargin

  def queryMappingFileAndInfo(mappingInfoFile:String): String =
    s"""
       |PREFIX tmp: <http://tmp-namespace.org/>
       |
       |SELECT DISTINCT *
       |WHERE {
       |?mapping a tmp:MappingFile .
       |
       |<$mappingInfoFile> tmp:hasDelimiter ?delimiter ;
       |	  tmp:hasQuotation ?quotation ;
       |    tmp:hasMappingFile ?mapping .
       |}
       |""".stripMargin
}
