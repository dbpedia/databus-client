package org.dbpedia.databus.client.sparql.queries

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

  def queryMappingInfoFile_old(sha: String): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |PREFIX dcat: <http://www.w3.org/ns/dcat#>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |PREFIX mapping: <
       |
       |SELECT DISTINCT ?mapping
       |WHERE {
       |  ?dataIdElement dataid:sha256sum "$sha"^^xsd:string .
       |  ?dataIdElement dataid:file ?file .
       |  ?mapping <http://tmp-namespace.org/databusFixRequired> ?file .
       |}
       |""".stripMargin

  def queryMappingInfoFile(sha: String): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |PREFIX dct:    <http://purl.org/dc/terms/>
       |PREFIX dcat:   <http://www.w3.org/ns/dcat#>
       |PREFIX db:     <https://databus.dbpedia.org/>
       |PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX map: <http://databus-client.tools.dbpedia.org/vocab/mapping/>
       |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
       |
       |SELECT DISTINCT ?mapping
       |WHERE {
       |  	?dataIdElement dataid:sha256sum "$sha"^^xsd:string .
       |  	?dataIdElement dataid:file ?file .
       |    ?dataset dcat:distribution ?dataIdElement .
       |    ?dataset dataid:artifact ?artifact .
       |  	OPTIONAL { ?mapping1 map:versionedFile ?file }
       |
       |    OPTIONAL {
       |		?mapping2 map:artifact ?artifact .
       |  		?mapping2 map:fileName ?fileName2 .
       |   	}
       |    FILTER (?fileName=STR(?fileName2))
       |    BIND( strafter ( STR(?dataIdElement) , "#") as ?fileName) .
       |
       |    OPTIONAL { ?mapping3 map:artifact ?artifact }
       |	BIND( coalesce(?mapping1, ?mapping2, ?mapping3) as ?mapping)
       |}
       |""".stripMargin

  def queryDownloadURLOfDatabusFiles(files: Seq[String]): String = {
    val databusFilesString = files.mkString("(<",">) (<",">)")
    s"""
       |PREFIX dcat:   <http://www.w3.org/ns/dcat#>
       |
       |SELECT DISTINCT ?file WHERE {
       |  	VALUES (?databusfile) {$databusFilesString}
       |  	?distribution ?o ?databusfile .
       |	  ?distribution dcat:downloadURL ?file .
       |}
       |""".stripMargin
  }
}
