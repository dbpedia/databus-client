package org.dbpedia.databus.sparql

import java.net.URL

import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.JenaRuntime
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}

object QueryHandler {

  val service = "https://databus.dbpedia.org/repo/sparql"

  def executeQuery(queryString: String, model:Model = ModelFactory.createDefaultModel()): Seq[QuerySolution] = {

    JenaRuntime.isRDF11 = false

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = {
      if(model.isEmpty) QueryExecutionFactory.sparqlService(service,query)
      else QueryExecutionFactory.create(query,model)
    }

//    println(query)

    var resultSeq: Seq[QuerySolution] = Seq.empty

    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val result = results.next()
        resultSeq = resultSeq :+ result
      }
    } finally qexec.close()

//    resultSeq.foreach(println(_))
    resultSeq
  }

  def executeDownloadQuery(queryString: String): Seq[String] = {

    val result = executeQuery(queryString)
    val sparqlVar = result.head.varNames().next()

    result.map(querySolution => querySolution.getResource(sparqlVar).toString)

  }

  def getSHA256Sum(url: String): String = {

    val results = executeQuery(DatabusQueries.querySha256(url))
    val sparqlVar = results.head.varNames().next()

    results.head.getLiteral(sparqlVar).toString

  }


  def downloadDataIdFile(url: String, dataIdFile: File): Boolean = {

    val result = executeQuery(DatabusQueries.queryDataId(url))

    if (result.nonEmpty) {
      val sparqlVar = result.head.varNames().next()
      val dataIdURL = result.head.getResource(sparqlVar).toString
      FileUtils.copyURLToFile(new URL(dataIdURL), dataIdFile.toJava)
      true
    }
    else{
      false
    }
  }



  def getTargetDir(dataIdFile: File, dest_dir: File): File = {
    val dataIdModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)

    val results = QueryHandler.executeQuery(DataIdQueries.queryDirStructure(), dataIdModel)
    val result = results.head

    //split the URI at the slashes and take the last cell
    val publisher = result.getResource("?publisher").toString.split("/").last.trim
    val group = result.getResource("?group").toString.split("/").last.trim
    val artifact = result.getResource("?artifact").toString.split("/").last.trim
    val version = result.getResource("?version").toString.split("/").last.trim

    dest_dir / publisher / group / artifact / version
  }

  def getFileExtension(fileURL: String, dataIdFile: File): String = {

    val query = DataIdQueries.queryFileExtension(fileURL)
    val model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)
    val result = executeQuery(query, model)

    if (result.nonEmpty) {
      val sparqlVar = result.head.varNames().next()
      result.head.getLiteral(sparqlVar).toString
    }
    else {
      ""
    }

  }

  def getMediaTypes(list: Seq[String]): Seq[String] = {
    val files = list.mkString("> , <")

    val queryStr = DatabusQueries.queryMediaType(files)
    val result = executeQuery(queryStr)

    val sparqlVar = result.head.varNames().next()

    result.map(querySolution => querySolution.getResource(sparqlVar).toString)
  }

  def getMappingInfoOf(sha: String): Seq[String] = {
    val queryStr = DatabusQueries.queryMappingInfo(sha)

    val results = executeQuery(queryStr)

    if (results.nonEmpty) {
      val sparqlVar = results.head.varNames().next()
      val mappingInfo = results.head.getResource(sparqlVar).toString
      println(s"MappingINFO: $mappingInfo")
      getMapping(mappingInfo)
    }
    else {
      Seq.empty[String]
    }
  }

  def getMapping(mappingInfo: String): Seq[String] = {
    val mappingModel: Model = RDFDataMgr.loadModel(mappingInfo, RDFLanguages.TURTLE)

    val queryStr = MappingQueries.queryMapping(mappingInfo)

    val result = executeQuery(queryStr, mappingModel).head

    val sparqlVar = result.varNames().next()

    val sparqlMap = result.getResource(sparqlVar).toString

    Seq(sparqlMap)
  }
}
