package archived.mapping

import better.files.File
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.sparql.QueryHandler
import org.scalatest.FlatSpec

class getMappingTest extends FlatSpec{
 "mappingInfo" should "be interpreted right to convert related csv" in {

   val mapInfo = "/home/eisenbahnplatte/git/format-mappings/tarql/1.ttl"

   val mapInfoModel =ModelFactory.createDefaultModel().read(mapInfo)

   val sparqlVar = "?mappingFile"
   val queryString =

   s"""
     |PREFIX tmp: <http://tmp-namespace.org/>
     |
     |SELECT DISTINCT $sparqlVar
     |WHERE {
     |<$mapInfo#this> tmp:hasMappingFile $sparqlVar .
     |}
     |""".stripMargin

   val result = executeMapQuery(queryString, mapInfoModel, sparqlVar)

   result.foreach(println(_))
 }

  def executeMapQuery(queryString: String, model:Model, sparqlVar:String): Seq[String] = {

    val query: Query = QueryFactory.create(queryString)
    //    println("\n--------------------------------------------------------\n")
    //    println(s"""Query:\n\n${query.toString()} """)

    val qexec: QueryExecution = QueryExecutionFactory.create(query,model)

    var filesSeq: Seq[String] = Seq[String]()

    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val resource = results.next().getResource(sparqlVar)
        filesSeq = filesSeq :+ resource.toString
      }
    } finally qexec.close()

    filesSeq
  }


  "executeQuery" should "be generic" in {

    val mapInfo = "/home/eisenbahnplatte/git/format-mappings/tarql/1.ttl"

    val mapInfoModel =ModelFactory.createDefaultModel().read(mapInfo)

    val sparqlVar = "?mappingFile"
    val mapComment = "?comment"
    val label = "?label"

    val varSeq = Seq(sparqlVar, label, mapComment)

    val queryString =
      s"""
         |PREFIX tmp: <http://tmp-namespace.org/>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |
         |SELECT DISTINCT *
         |WHERE {
         |<$mapInfo#this> tmp:hasMappingFile $sparqlVar ;
         |rdfs:label $label;
         |rdfs:comment $mapComment .
         |}
         |""".stripMargin

    val result = executeQuery(queryString, mapInfoModel, varSeq)

    result.foreach(println(_))
  }

  def executeQuery(queryString: String, model:Model = ModelFactory.createDefaultModel(), sparqlVars:Seq[String]): Seq[QuerySolution] = {

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.create(query,model)

    var resultSeq: Seq[QuerySolution] = Seq.empty

    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val result = results.next()
        resultSeq = resultSeq :+ result
      }
    } finally qexec.close()

    resultSeq
  }

  "model" should "be empty" in {
    val model:Model = ModelFactory.createDefaultModel()

    println(model.isEmpty)
    assert(model.isEmpty)
  }

//  "conversionTests/format.mapping" should "be retourned" in {
//    val path = "/home/eisenbahnplatte/git/databus-client/src/resources/mappingTests/getMapping/bnetza-mastr_rli_type=hydro.csv.bz2"
//    val sha = FileUtil.getSha256(File(path))
//    println(sha)
//    println(QueryHandler.getMapping(sha))
//  }

  "mappingInfo" should "return format.mapping" in {

    val mappingInfo = "https://raw.githubusercontent.com/dbpedia/format-mappings/master/tarql/1.ttl#this"
    val model = RDFDataMgr.loadModel(mappingInfo)

    val stmts= model.listStatements()
    while (stmts.hasNext) println(stmts.nextStatement())


    val queryStr =
      s"""
        |PREFIX tmp: <http://tmp-namespace.org/>
        |
        |SELECT DISTINCT ?format.mapping
        |WHERE {
        |?format.mapping a tmp:MappingFile .
        |<$mappingInfo> tmp:hasMappingFile ?format.mapping .
        |}
        |""".stripMargin

    QueryHandler.executeQuery(queryStr,model).foreach(x=> println(s"sol: $x"))
  }

  "Query" should "return other values when one values is missing" in {

    val mappingInfoFile = "https://raw.githubusercontent.com/dbpedia/format-mappings/master/tarql/1.ttl#this"

    val model = RDFDataMgr.loadModel(mappingInfoFile)

    val queryStr =
      s"""
         |PREFIX tmp: <http://tmp-namespace.org/>
         |
        |SELECT DISTINCT *
         |WHERE {
         |?format.mapping a tmp:MappingFile ;
         |    tmp:hasDelimiter ?delimiter ;
         |	  tmp:hasQuotation ?quotation .
         |<$mappingInfoFile> tmp:hasMappingFile ?format.mapping .
         |}
         |""".stripMargin

    val result = QueryHandler.executeQuery(queryStr,model)

    result.head.varNames()
  }
}
