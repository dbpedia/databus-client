package org.dbpedia.databus.client.filehandling.convert.mapping

class MappingInfo(mapFile: String, del: Character=',', quo: Character='"') {
  val mappingFile: String = mapFile
  val delimiter: Character = del
  var quotation: Character = quo
}

