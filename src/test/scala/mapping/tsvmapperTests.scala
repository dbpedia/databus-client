package mapping

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.shared.impl.PrefixMappingImpl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.scalatest.FlatSpec

class tsvmapperTests extends FlatSpec {

  "tarql" should "read out all data from tsv file" in {

    def tsv_read() = {
      val prefixes = new PrefixMappingImpl()
      val tarqlquery = new TarqlParser("/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/select.sparql").getResult
      val csvFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/test.csv"
      val resultSet = TarqlQueryExecutionFactory.create(tarqlquery, csvFilePath, null).execSelect()


      while (resultSet.hasNext) println(resultSet.next)

    }

    tsv_read()

    def selectSparql() = {
      val tarqlQuery = new TarqlParser("/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/select.sparql").getResult
      val resultSet = TarqlQueryExecutionFactory.create(tarqlQuery).execSelect()

      while (resultSet.hasNext) println(resultSet.next())
    }

    println("select")
    selectSparql()

    def tsvmap() = {
      val tarqlQuery = new TarqlParser("/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/mapping.sparql").getResult

      println(tarqlQuery.isConstructType)
      val rs = TarqlQueryExecutionFactory.create(tarqlQuery).execTriples()

      while (rs.hasNext) println(rs.next())
    }

    println("tsvmap")
    tsvmap()
  }

  "tarql" should "read all data of testWithoutSlashes.csv" in {
    val inputFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testWithoutSlashes.csv"
    val mappingFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testWithoutSlashes.sparql"
    tarqlMapCSV(inputFilePath, mappingFilePath)
  }

  "tarql" should "read all data of test2.csv" in {
    val inputFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/test2.csv"
    val mappingFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/test2.sparql"
    tarqlMapCSV(inputFilePath, mappingFilePath)
  }

  def tarqlMapCSV(inputFile: String, mappingFile: String): Unit = {
    val tarqlquery = new TarqlParser(mappingFile).getResult
    val rs = TarqlQueryExecutionFactory.create(tarqlquery, inputFile).execTriples()
    while (rs.hasNext) println(rs.next())
  }


  "tarql" should "read all data of testBob.csv" in {
    val inputFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testBob.csv"
    val mappingFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testBob.sparql"
    tarqlMapCSV(inputFilePath, mappingFilePath)
  }

  "tarql" should "read all data of testBob.TSV" in {
    val inputFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testBob.tsv"
    val mappingFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testBob.sparql"
    tarqlMapTSV(inputFilePath, mappingFilePath)
  }

  def tarqlMapTSV(inputFile: String, mappingFile: String): Unit = {
    val tarqlquery = new TarqlParser(mappingFile).getResult
    val rs = TarqlQueryExecutionFactory.create(tarqlquery, inputFile, CSVOptions.withTSVDefaults()).execTriples()
    while (rs.hasNext) println(rs.next())
  }


  "tarql" should "read all data of testBob.TSV and load them into RDD[Triple]" in {
    val spark = SparkSession.builder()
      .appName(s"Triple reader")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testBob.tsv"
    val mappingFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/testBob.sparql"
    val rdd = tarqlMapTSVtoRDD(inputFilePath, mappingFilePath, sc)
    rdd.foreach(println(_))

    var triple = Triple.create(NodeFactory.createBlankNode(), NodeFactory.createBlankNode(), NodeFactory.createBlankNode())
    rdd.take(1).foreach(x => triple = x)
    assert(triple.getClass.getSimpleName == "Triple")
    println(rdd.count())
    assert(rdd.count() == 9)

  }

  def tarqlMapTSVtoRDD(inputFile: String, mappingFile: String, sc: SparkContext): RDD[Triple] = {

    var rdd = sc.emptyRDD[Triple]

    val tarqlquery = new TarqlParser(mappingFile).getResult
    val rs = TarqlQueryExecutionFactory.create(tarqlquery, inputFile, CSVOptions.withTSVDefaults()).execTriples()
    while (rs.hasNext) rdd = sc.union(rdd, sc.parallelize(Seq(rs.next())))
    rdd
  }
}
