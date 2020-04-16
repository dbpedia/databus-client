package org.dbpedia.databus.client.sparql

import java.net.URL

import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.JenaRuntime
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.dbpedia.databus.client.sparql.queries.{DataIdQueries, DatabusQueries, MappingQueries}

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

    results.head.getLiteral(sparqlVar).getString

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

  def getTargetDir(dataIdFile: File): String = {
    val dataIdModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)

    val results = QueryHandler.executeQuery(DataIdQueries.queryDirStructure(), dataIdModel)
    val result = results.head

    //split the URI at the slashes and take the last cell
    val publisher = result.getResource("?publisher").toString.split("/").last.trim
    val group = result.getResource("?group").toString.split("/").last.trim
    val artifact = result.getResource("?artifact").toString.split("/").last.trim
    val version = result.getResource("?version").toString.split("/").last.trim

    s"$publisher/$group/$artifact/$version"
  }

  def getFileExtension(fileURL: String, dataIdFile: File): String = {

    val query = DataIdQueries.queryFileExtension(fileURL)
    val model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)
    val result = executeQuery(query, model)

    if (result.nonEmpty) {
      val sparqlVar = result.head.varNames().next()
      result.head.getLiteral(sparqlVar).getString
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

  def getMapping(sha: String): Seq[String] = {

    val results = executeQuery(
      DatabusQueries.queryMappingInfoFile(sha)
    )

    if (results.nonEmpty) {
      val sparqlVar = results.head.varNames().next()
      val mappingInfoFile = results.head.getResource(sparqlVar).toString
      println(s"MappingInfoFile: $mappingInfoFile")
      getMappingFileAndInfo(mappingInfoFile)
    }
    else {
      Seq.empty[String]
    }
  }

  def getMappingFileAndInfo(mappingInfoFile: String): Seq[String] = {
    val mappingModel: Model = RDFDataMgr.loadModel(mappingInfoFile, RDFLanguages.TURTLE)

    val result = executeQuery(
      MappingQueries.queryMappingFileAndInfo(mappingInfoFile),
      mappingModel
    )

    if (result.nonEmpty) {
      Seq[String](
        result.head.getResource("mapping").toString,
        result.head.getLiteral("delimiter").getString,
        result.head.getLiteral("quotation").getString)
    }
    else {
      val result = executeQuery(
        MappingQueries.queryMappingFile(mappingInfoFile),
        mappingModel
      ).head

      val sparqlVar = result.varNames().next()
      val tarqlMapFile = result.getResource(sparqlVar).toString

      Seq[String](tarqlMapFile)
    }
  }
}
