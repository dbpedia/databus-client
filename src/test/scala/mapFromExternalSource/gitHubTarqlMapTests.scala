package mapFromExternalSource

import org.apache.jena.query.{Query, QueryExecution, QueryExecutionFactory, QueryFactory, ResultSet}
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.scalatest.FlatSpec

class gitHubTarqlMapTests extends FlatSpec{
  "Sparql Query" should "necessary data for mapping" in {

    val url = "https://raw.githubusercontent.com/dbpedia/format-mappings/master/tarql/1.ttl"
    val model = RDFDataMgr.loadModel(url,Lang.TTL)
    val liststmt = model.listStatements()
    while (liststmt.hasNext) println(liststmt.nextStatement())

    model.listNameSpaces()
    println(model.listSubjects().toList.toArray.filter(x => x.toString.contains("#this")).last)

    val queryString =
      s"""
        |PREFIX tmp: <http://tmp-namespace.org/>
        |
        |SELECT ?mapFile ?mainArtifact ?mainFile ?delimiter ?containsHeader
        |WHERE {
        |<$url#this> tmp:hasMappingFile ?mapFile .
        |?mapFile a tmp:MappingFile .
        |<$url#this> tmp:mainArtifact ?mainArtifact .
        |<$url#this> tmp:mainFile ?mainFile .
        |#<$url#this> tmp:delimiter ?delimiter .
        |#<$url#this> tmp:containsHeader ?containsHeader .
        |}
        |""".stripMargin
    val query: Query = QueryFactory.create(queryString)
    println(query)

    val qexec: QueryExecution = QueryExecutionFactory.create(query,model)

    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val result= results.next()
        val mapFile = result.getResource("?mapFile")
        val mainFile = result.getLiteral("?mainFile")
        val mainArtifact = result.getResource("?mainArtifact")
        val delimiter = ""//result.getLiteral("?delimiter")
        val containsHeader = ""//result.getLiteral("?containsHeader")
        println(s"$mapFile , $mainFile , $mainArtifact , $delimiter, $containsHeader")
      }
    } finally qexec.close()

  }

  "Part of URI" should "selected" in {
    val url = "https://raw.githubusercontent.com/dbpedia/format-mappings/master/tarql/1.ttl"
    val model = RDFDataMgr.loadModel(url,Lang.TTL)

    val queryString =
      s"""
         |PREFIX tmp: <http://tmp-namespace.org/>
         |
         |SELECT ?mappingDocu ?artifact ?var
         |WHERE {
         |BIND(IRI(CONCAT(STR(?mappingDocu), "#this")) AS ?var)
         |?var tmp:mainArtifact ?artifact .
         |#<?mappingDocu#this> tmp:mainFile ?file
         |#?mapFile a tmp:MappingFile .
         |}
         |""".stripMargin
    val query: Query = QueryFactory.create(queryString)
    println(query)

    val qexec: QueryExecution = QueryExecutionFactory.create(query,model)



    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val result= results.next()
        val mapFile = result.getResource("?mappingDocu")
        val bindedVar = result.getResource("?var")
        val artifact = result.getResource("?artifact")
        println(s"$mapFile $artifact, $bindedVar")
        println(result)
      }
    } finally qexec.close()

  }

  "DownloadURL" should "be enough information to get the mappingDocu file" in {
    val downloadURL = ""

    val queryString =
      s"""
         |PREFIX tmp: <http://tmp-namespace.org/>
         |
         |SELECT ?mappingDocu
         |WHERE {
         |IRI(CONCAT(STR(?mappingDocu), "#this)) tmp:mainArtifact ?artifact .
         |#<?mappingDocu#this> tmp:mainFile ?file
         |#?mapFile a tmp:MappingFile .
         |}
         |""".stripMargin
    val query: Query = QueryFactory.create(queryString)
    println(query)

    val qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)



    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val result= results.next()
        val mapFile = result.getResource("?mapFile")
        val mainFile = result.getLiteral("?mainFile")
        val mainArtifact = result.getResource("?mainArtifact")
        val delimiter = ""//result.getLiteral("?delimiter")
        val containsHeader = ""//result.getLiteral("?containsHeader")
        println(s"$mapFile , $mainFile , $mainArtifact , $delimiter, $containsHeader")
      }
    } finally qexec.close()

  }

}
