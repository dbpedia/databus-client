package org.dbpedia.databus.sparql

import java.net.URL

import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}

object QueryHandler {

  val service = "https://databus.dbpedia.org/repo/sparql"

  def executeDownloadQuery(queryString: String): Seq[String] = {

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService(service, query)

    var filesSeq: Seq[String] = Seq[String]()

    try {
      val results: ResultSet = qexec.execSelect
      while (results.hasNext) {
        val resource = results.next().getResource("?file")
        filesSeq = filesSeq :+ resource.toString
      }
    } finally qexec.close()

    filesSeq
  }

  def getMapping(url: String, dataIdFile: File): Unit = {
    val queryString =
      s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>
                    SELECT DISTINCT ?dataset WHERE {
                    ?dataset dataid:version ?version .
                    ?dataset dcat:distribution ?distribution .
                    ?distribution dcat:downloadURL <$url> }"""

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService(service, query)

    try {
      val results: ResultSet = qexec.execSelect

      if (results.hasNext) {
        val dataidURL = results.next().getResource("?dataset").toString
        //        println(dataidURL)
        FileUtils.copyURLToFile(new URL(dataidURL), dataIdFile.toJava)
      }
    } finally qexec.close()

  }

  def downloadDataIdFile(url: String, dataIdFile: File): Unit = {
    val queryString =
      s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>
                    SELECT DISTINCT ?dataset WHERE {
                    ?dataset dataid:version ?version .
                    ?dataset dcat:distribution ?distribution .
                    ?distribution dcat:downloadURL <$url> }"""

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService(service, query)

    try {
      val results: ResultSet = qexec.execSelect

      if (results.hasNext) {
        val dataidURL = results.next().getResource("?dataset").toString
        //        println(dataidURL)
        FileUtils.copyURLToFile(new URL(dataidURL), dataIdFile.toJava)
      }
    } finally qexec.close()

  }

  def executeQuery(queryString: String, model:Model = ModelFactory.createDefaultModel()): Seq[QuerySolution] = {

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = {
      if(model.isEmpty) QueryExecutionFactory.sparqlService(service,query)
      else QueryExecutionFactory.create(query,model)
    }

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

  def getSHA256Sum(url: String): String = {

    val sparqlVar = "?sha256sum"

    val queryString =
      s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
         |PREFIX dcat:   <http://www.w3.org/ns/dcat#>
         |SELECT $sparqlVar  WHERE {
         |  ?s dcat:downloadURL <$url>  .
         |  ?s dataid:sha256sum $sparqlVar .
         |}""".stripMargin

    val results = executeQuery(queryString)

    results(0).get(sparqlVar).toString

  }

  def getTargetDir(dataIdFile: File, dest_dir: File): File = {
    val dataIdModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)

    val results = QueryHandler.executeQuery(DataIdQueries.dirStructureQuery(), dataIdModel)
    val result = results(0)

    //split the URI at the slashes and take the last cell
    val publisher = result.getResource("?publisher").toString.split("/").last.trim
    val group = result.getResource("?group").toString.split("/").last.trim
    val artifact = result.getResource("?artifact").toString.split("/").last.trim
    val version = result.getResource("?version").toString.split("/").last.trim

    dest_dir / publisher / group / artifact / version
  }

  def getTypeOfFile(fileURL: String, dataIdFile: File): String = {
    var fileType = ""

    val dataidModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)

    val query: Query = QueryFactory.create(DataIdQueries.queryGetType(fileURL))
    val qexec = QueryExecutionFactory.create(query, dataidModel)

    try {
      val results = qexec.execSelect
      if (results.hasNext) {
        fileType = results.next().getLiteral("?type").toString
      }
    } finally qexec.close()

    fileType
  }

  def getMediatypesOfQuery(list: List[String]): Array[String] = {
    val files = list.mkString("> , <")
    var mediaTypes = Array.empty[String]
    println(files)
    val queryString =
      s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>
                    SELECT DISTINCT ?type WHERE {
                    ?distribution dcat:mediaType ?type .
                    ?distribution dcat:downloadURL ?du .
                    FILTER (?du in (<$files>))
                    }
                    GROUP BY ?type"""

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService(service, query)


    try {
      val results: ResultSet = qexec.execSelect

      while (results.hasNext) {
        val mediaType = results.next().getResource("?type").toString
        mediaTypes = mediaTypes :+ mediaType
      }
    } finally qexec.close()

    mediaTypes.foreach(x => println(s"MediaType: $x"))
    mediaTypes
  }
}
