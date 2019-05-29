package org.dbpedia.databus

import org.apache.jena.query._

object SelectQuery {

  def execute(queryString:String) {
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


}
