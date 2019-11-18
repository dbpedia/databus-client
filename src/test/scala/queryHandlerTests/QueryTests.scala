package queryHandlerTests

import better.files.File
import org.dbpedia.databus.sparql.QueryHandler
import org.scalatest.FlatSpec

class QueryTests extends FlatSpec{

  "QueryHandler" should "return right TargetDir" in {
    val dataID= File("dataId_test.ttl")
    val dest_dir = File("test")
    QueryHandler.downloadDataIdFile("https://databus.dbpedia.org/data/databus/databus-data/2019.11.10/databus-data.nt.bz2", dataID)
    val targetDir = QueryHandler.getTargetDir(dataID, dest_dir)

    val comparisonDir = File("test") / "dbpedia" / "databus" / "databus-data" / "2019.11.10"
    assert(comparisonDir == targetDir)
  }

  "QueryHandler" should "return right dirList" in {
    val dataID= File("dataId_test")
    QueryHandler.downloadDataIdFile("https://databus.dbpedia.org/data/databus/databus-data/2019.11.10/databus-data.nt.bz2", dataID)
//    val targetDir = QueryHandler.executeDataIdQuery(dataID)

//    targetDir.foreach(println(_))

    val comparisonDir = File("test") / "dbpedia" / "databus" / "databus-data" / "2019.11.10"
//    assert(comparisonDir == targetDir)
  }

}
