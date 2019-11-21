package mapping

import java.io.StringReader

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class multipleConstructs extends FlatSpec{

  "tarql" should "map tsv file with mappingFile that includes multiple Subjects" in {
    val spark:SparkSession = SparkSession.builder()
      .appName(s"Triple reader")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val inFile = "./src/resources/mappingTests/test/global-ids_base58.tsv"
    val mappingFile = "./src/resources/mappingTests/test/global-ids_base58.sparql"

    val tarqlparser = new TarqlParser(mappingFile)
    println(tarqlparser.getResult.getQueries.size())

//    val data = TarqlQueryExecutionFactory.create(tarqlparser.getResult, inFile, CSVOptions.withTSVDefaults()).execTriples()
//    while(data.hasNext) println(data.next())

    val data2 = TSV_Reader.csv_to_rdd(mappingFile, inFile, "\t", spark.sparkContext)

//    val data =TSV_Reader.csv_to_rdd(mappingFile, inFile, "\t", spark.sparkContext)

    data2.foreach(println(_))
  }

  "tarql" should "accept multiple Constructs" in {
    val inFile = "./src/resources/mappingTests/test/global-ids_base58.tsv"

    val s =
      """
        |CONSTRUCT {
        |?URIresource <http://www.w3.org/2002/07/owl#sameAs> ?URIsingleton;
        |	<http://www.w3.org/2002/07/owl#partOfCluster> ?URIcluster.
        |
        |}
        |WHERE {
        |    BIND(URI(?original_iri) AS ?URIresource)
        |    BIND(URI(CONCAT('https://global.dbpedia.org/id/',?singleton_id_base58)) AS ?URIsingleton)
        |    BIND(URI(CONCAT('https://global.dbpedia.org/id/',?cluster_id_base58)) AS ?URIcluster)
        |}
        |CONSTRUCT {
        |?URIsingleton <http://www.w3.org/2002/07/owl#partOfCluster> ?URIcluster.
        |}
        |WHERE {
        |    BIND(URI(CONCAT('https://global.dbpedia.org/id/',?singleton_id_base58)) AS ?URIsingleton)
        |    BIND(URI(CONCAT('https://global.dbpedia.org/id/',?cluster_id_base58)) AS ?URIcluster)
        |}
        |""".stripMargin

    val tarqlparser = new TarqlParser(new StringReader(s))
    println(tarqlparser.getResult.getQueries.size())

    val data = TarqlQueryExecutionFactory.create(tarqlparser.getResult, inFile, CSVOptions.withTSVDefaults()).execTriples()
    while(data.hasNext) println(data.next())
  }
}

object TSV_Reader {

  def csv_to_rdd(mapFile: String, csvFilePath: String = "", delimiter: String = ",", sc: SparkContext): RDD[Triple] = {

    val tarqlQuery = new TarqlParser(mapFile)

    val csvOptions = {
      if (delimiter == ",") CSVOptions.withCSVDefaults()
      else if (delimiter == "\t") CSVOptions.withTSVDefaults()
      else null
    }

    var seq: Seq[Triple] = Seq.empty

    if (csvOptions != null) {
      val resultSet = csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlQuery.getResult).execTriples()
        case _ => TarqlQueryExecutionFactory.create(tarqlQuery.getResult, csvFilePath, csvOptions).execTriples()
      }

      while (resultSet.hasNext) seq = seq :+ resultSet.next()

    }
    else {
      LoggerFactory.getLogger("read_CSV").error(s"Delimiter: $delimiter not supported")
      println(s"ERROR (read_CSV): Delimiter: $delimiter not supported")
    }
    sc.parallelize(seq)
  }
}