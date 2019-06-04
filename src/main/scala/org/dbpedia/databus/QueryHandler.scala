package org.dbpedia.databus

import better.files.File
import org.apache.jena.query._
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.dbpedia.databus.sparql.DataIdQueries

object QueryHandler {

  def executeSelectQuery(queryString:String) = {
    var query: Query = QueryFactory.create(queryString)
    var qexec: QueryExecution = QueryExecutionFactory.sparqlService("http://databus.dbpedia.org/repo/sparql", query)


    try {
      var results: ResultSet = qexec.execSelect
      var fileHandler = FileHandler

      while (results.hasNext()) {
        var resource = results.next().getResource("?file")
        fileHandler.downloadFile(resource.toString())
      }
    } finally qexec.close()
  }

  def executeDataIdQuery(dataIdFile:File):List[String] ={

    val dataidModel: Model = RDFDataMgr.loadModel(dataIdFile.pathAsString,RDFLanguages.NTRIPLES)

    //dir_structure : publisher/group/artifact/version
    var dir_structure = List[String]()

    var query: Query = QueryFactory.create(DataIdQueries.queryGetPublisher())
    var qexec = QueryExecutionFactory.create(query,dataidModel)

    try {
      val results = qexec.execSelect
      if (results.hasNext()) {
        var publisher = results.next().getResource("?o").toString().split("/").map(_.trim).last
        dir_structure = dir_structure :+ publisher
      }
    } finally qexec.close()

    query = QueryFactory.create(DataIdQueries.queryGetGroup())
    qexec = QueryExecutionFactory.create(query,dataidModel)

    try {
      val results = qexec.execSelect
      if (results.hasNext()) {
        var group = results.next().getResource("?o").toString().split("/").map(_.trim).last
        dir_structure = dir_structure :+ group
      }
    } finally qexec.close()

    query = QueryFactory.create(DataIdQueries.queryGetArtifact())
    qexec = QueryExecutionFactory.create(query,dataidModel)

    try {
      val results = qexec.execSelect
      if (results.hasNext()) {
        var artifact = results.next().getResource("?o").toString().split("/").map(_.trim).last
        dir_structure = dir_structure :+ artifact
      }
    } finally qexec.close()

    query = QueryFactory.create(DataIdQueries.queryGetVersion())
    qexec = QueryExecutionFactory.create(query,dataidModel)

    try {
      val results = qexec.execSelect
      if (results.hasNext()) {
        var version = results.next().getResource("?o").toString().split("/").map(_.trim).last
        dir_structure = dir_structure :+ version
      }
    } finally qexec.close()

    return dir_structure
  }



}
