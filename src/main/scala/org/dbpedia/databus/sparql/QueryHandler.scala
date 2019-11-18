package org.dbpedia.databus.sparql

import java.net.URL

import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.query._
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}

object QueryHandler {

  def executeDownloadQuery(queryString: String): Seq[String] = {

    val query: Query = QueryFactory.create(queryString)
//    println("\n--------------------------------------------------------\n")
//    println(s"""Query:\n\n$query""")

    val qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)


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

  def downloadDataIdFile(url: String, dataIdFile: File): Unit = {
    val queryString =
      s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>
                    SELECT DISTINCT ?dataset WHERE {
                    ?dataset dataid:version ?version .
                    ?dataset dcat:distribution ?distribution .
                    ?distribution dcat:downloadURL <$url> }"""

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)

    try {
      val results: ResultSet = qexec.execSelect

      if (results.hasNext) {
        val dataidURL = results.next().getResource("?dataset").toString
        //        println(dataidURL)
        FileUtils.copyURLToFile(new URL(dataidURL), dataIdFile.toJava)
      }
    } finally qexec.close()

  }

  def getSHA256Sum(url: String): String = {
    var sha256 = ""
    val queryString =
      s"""PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
         |PREFIX dcat:   <http://www.w3.org/ns/dcat#>
         |SELECT ?sha256sum  WHERE {
         |  ?s dcat:downloadURL <$url>  .
         |  ?s dataid:sha256sum ?sha256sum .
         |}""".stripMargin

    val query: Query = QueryFactory.create(queryString)
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)

    try {
      val results: ResultSet = qexec.execSelect

      if (results.hasNext) {
        sha256 = results.next().getLiteral("?sha256sum").toString
      }
    } finally qexec.close()

    sha256
  }

  def getTargetDir(dataIdFile: File, dest_dir: File): File = {
    val dataIdModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString, RDFLanguages.TURTLE)
    val query: Query = QueryFactory.create(DataIdQueries.queryGetDirStructure())
    val qexec = QueryExecutionFactory.create(query, dataIdModel)

    var targetDir = File("")

    try {
      val results = qexec.execSelect
      if (results.hasNext) {
        val result = results.next()
        //split the URI at the slashes and take the last cell
        val publisher = result.getResource("?publisher").toString.split("/").last.trim
        val group = result.getResource("?group").toString.split("/").last.trim
        val artifact = result.getResource("?artifact").toString.split("/").last.trim
        val version = result.getResource("?version").toString.split("/").last.trim
        targetDir = dest_dir / publisher / group / artifact / version
      }
    } finally qexec.close()

    targetDir
  }

  //  def executeDataIdQuery(dataIdFile: File): List[String] = {
  //
  //    val dataidModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString)
  //
  //    //dir_structure : publisher/group/artifact/version
  //    var dir_structure = List[String]()
  //
  //    var query: Query = QueryFactory.create(DataIdQueries.queryGetPublisher())
  //    var qexec = QueryExecutionFactory.create(query, dataidModel)
  //
  //    try {
  //      val results = qexec.execSelect
  //      if (results.hasNext) {
  //        //split the URI at the slashes and take the last cell
  //        val publisher = results.next().getResource("?o").toString.split("/").map(_.trim).last
  //        dir_structure = dir_structure :+ publisher
  //      }
  //    } finally qexec.close()
  //
  //    query = QueryFactory.create(DataIdQueries.queryGetGroup())
  //    qexec = QueryExecutionFactory.create(query, dataidModel)
  //
  //    try {
  //      val results = qexec.execSelect
  //      if (results.hasNext) {
  //        val group = results.next().getResource("?o").toString.split("/").map(_.trim).last
  //        dir_structure = dir_structure :+ group
  //      }
  //    } finally qexec.close()
  //
  //    query = QueryFactory.create(DataIdQueries.queryGetArtifact())
  //    qexec = QueryExecutionFactory.create(query, dataidModel)
  //
  //    try {
  //      val results = qexec.execSelect
  //      if (results.hasNext()) {
  //        val artifact = results.next().getResource("?o").toString().split("/").map(_.trim).last
  //        dir_structure = dir_structure :+ artifact
  //      }
  //    } finally qexec.close()
  //
  //    query = QueryFactory.create(DataIdQueries.queryGetVersion())
  //    qexec = QueryExecutionFactory.create(query, dataidModel)
  //
  //    try {
  //      val results = qexec.execSelect
  //      if (results.hasNext()) {
  //        val version = results.next().getResource("?o").toString().split("/").map(_.trim).last
  //        dir_structure = dir_structure :+ version
  //      }
  //    } finally qexec.close()
  //
  //    return dir_structure
  //  }

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
    val qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)


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
