package org.dbpedia.databus.client.sparql

import java.io.FileNotFoundException
import java.net.URL
import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.JenaRuntime
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.dbpedia.databus.client.filehandling.convert.mapping.util.MappingInfo
import org.dbpedia.databus.client.sparql.queries.{DataIdQueries, DatabusQueries, MappingQueries}
import org.slf4j.{Logger, LoggerFactory}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty

class ClientConfig {
  @BeanProperty var endpoint = ""
}

object QueryHandler {


  val service:String = readYamlConfig(File("config.yml")).endpoint

  def readYamlConfig(file: File): ClientConfig = {
    val yaml = new Yaml(new Constructor(classOf[ClientConfig]))
    yaml.load(file.newFileInputStream).asInstanceOf[ClientConfig]
  }

  val logger: Logger = LoggerFactory.getLogger(getClass)

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

    resultSeq
  }

  def executeDownloadQuery(queryString: String): Seq[String] = {

    val result = executeQuery(queryString)

    println(result)
    val sparqlVar = result.head.varNames().next()

    result.map(querySolution => querySolution.getResource(sparqlVar).toString)

  }

  def getSHA256Sum(url: String): String = {

    val results = executeQuery(DatabusQueries.querySha256(url))

    try{
      val sparqlVar = results.head.varNames().next()
      results.head.getLiteral(sparqlVar).getString
    } catch {
      case noSuchElementException: NoSuchElementException =>
        logger.error(s"No Sha Sum found for $url")
        ""
    }

  }

  def getOutputFile(url:String): File ={
    val results = executeQuery(DatabusQueries.querySha256(url))

    try{
      val sparqlVar = results.head.varNames().next()
      results.head.getLiteral(sparqlVar).getString
    } catch {
      case noSuchElementException: NoSuchElementException =>
        logger.error(s"No Sha Sum found for $url")
        File("")
    }
  }


  def downloadDataIdFile(url: String, dataIdFile: File): Boolean = {

    val result = executeQuery(DatabusQueries.queryDataId(url))

    if (result.nonEmpty) {
      val sparqlVar = result.head.varNames().next()
      val dataIdURL = result.head.getResource(sparqlVar).toString

      try {
        FileUtils.copyURLToFile(new URL(dataIdURL), dataIdFile.toJava)
        true
      } catch {
        case fileNotFoundException: FileNotFoundException =>
          LoggerFactory.getLogger("DownloadLogger").error(s"dataID URL: $dataIdURL not found.")
          false
      }

    }
    else{
      false
    }
  }

  def getTargetDir(dataIdFile: File): String = {
    val dataIdModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.JSONLD)

    val stmts = dataIdModel.listStatements()
    while (stmts.hasNext) println(stmts.next())
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
    val model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.JSONLD)
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

  def getPossibleMappings(sha: String): Seq[String] = {

    val results = executeQuery(
      DatabusQueries.queryMappingInfoFile(sha)
    )

    if (results.nonEmpty) {
      val sparqlVar = results.head.varNames().next()
      val possibleMappings = {
        try {
          results.map(solution => solution.getResource(sparqlVar).toString)
        } catch {
          case nullPointerException: NullPointerException =>
            logger.warn("No Mapping found for")
            Seq.empty[String]
        }
      }

      println(s"possible MappingInfoFile's:")
      possibleMappings.foreach(println(_))

//      getMappingFileAndInfo(mappingInfoFile)
      possibleMappings
    }
    else {
      Seq.empty[String]
    }
  }

  def getMappingFileAndInfo(mappingInfoFile: String): MappingInfo = {
    val mappingModel: Model = RDFDataMgr.loadModel(mappingInfoFile, RDFLanguages.TURTLE)

    val result = executeQuery(
      MappingQueries.queryMappingFileAndInfo(mappingInfoFile),
      mappingModel
    )

    if (result.nonEmpty) {
      new MappingInfo(
        result.head.getResource("mapping").toString,
        result.head.getLiteral("delimiter").getString,
        result.head.getLiteral("quotation").getString
      )
    }
    else {
      val result = executeQuery(
        MappingQueries.queryMappingFile(mappingInfoFile),
        mappingModel
      ).head

      val sparqlVar = result.varNames().next()
      val tarqlMapFile = result.getResource(sparqlVar).toString

      new MappingInfo(tarqlMapFile)
    }
  }
}
