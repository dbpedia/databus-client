package mapping

import better.files.File
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.RDFFormat
import org.apache.jena.shared.impl.PrefixMappingImpl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.filehandling.converter.rdf_writer.RDF_Writer
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.scalatest.FlatSpec

class tsvmapperTests extends FlatSpec {

  val testDir:String = "./src/resources/test/MappingTests/read/"

  val spark:SparkSession = SparkSession.builder()
    .appName(s"Triple reader")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  def csv_map(mapFile:String, csvFilePath:String = "", isTsvFile:Boolean=false, map:Boolean=true): Unit = {

    val prefixes = new PrefixMappingImpl()
    val tarqlquery = new TarqlParser(mapFile).getResult
    val csvOptions = {
      if (isTsvFile) CSVOptions.withTSVDefaults()
      else CSVOptions.withCSVDefaults()
    }

    val resultSet = if(map) {
      csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlquery).execTriples()
        case _ => TarqlQueryExecutionFactory.create(tarqlquery, csvFilePath, csvOptions).execTriples()
      }
    }
    else
    { csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlquery).execSelect()
        case _ => TarqlQueryExecutionFactory.create(tarqlquery, csvFilePath, csvOptions).execSelect()
      }
    }

    while (resultSet.hasNext) println(resultSet.next)
  }

  def csv_map_to_rdd(mapFile:String, csvFilePath:String = "", delimiter:String="," , sc: SparkContext): RDD[Triple] = {

    val prefixes = new PrefixMappingImpl()
    val tarqlquery = new TarqlParser(mapFile).getResult

    val csvOptions = {
      if (delimiter==",") CSVOptions.withCSVDefaults()
      else if (delimiter=="\t") CSVOptions.withTSVDefaults()
      else null
    }

    var rdd = sc.emptyRDD[Triple]

    if (csvOptions != null) {
      val resultSet =  csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlquery).execTriples()
        case _ => TarqlQueryExecutionFactory.create(tarqlquery, csvFilePath, csvOptions).execTriples()
      }

      while (resultSet.hasNext) rdd = sc.union(rdd, sc.parallelize(Seq(resultSet.next())))
    } else {
      println(s"DELIMITER WIRD NICHT UNTERSTUEZT: $delimiter")
    }

    rdd
  }

  "tarql" should "read out all data from csv file" in {

    println("SELECT WITH CSV FILE PATH")
    csv_map(s"${testDir}test1_select.sparql",s"${testDir}test1.csv",map=false)

    println("SELECT WITH CSV FILE PATH IMPLEMENTED IN MAPFILE")
    csv_map(s"${testDir}test1_select2.sparql",map=false)

  }

  "tarql" should "map all data to triples" in{

    println("TARQLMAPPING")
    csv_map(s"${testDir}test1_mapping.sparql")

  }

  "tarql" should "read all data of test2.csv as Triples" in {
    val inputFilePath = s"${testDir}test2.csv"
    val mappingFilePath = s"${testDir}test2.sparql"
    csv_map(mappingFilePath,inputFilePath)
  }

  "tarql" should "read all data of testBob.TSV" in {
    val inputFilePath = s"${testDir}testBob.tsv"
    val mappingFilePath = s"${testDir}testBob.sparql"
    csv_map(mappingFilePath, inputFilePath,  isTsvFile = true)
  }

  "tarql" should "read all data of testBob.TSV and load them into RDD[Triple]" in {

    val inputFilePath = s"${testDir}testBob.tsv"
    val mappingFilePath = s"${testDir}testBob.sparql"

    val rdd = csv_map_to_rdd(mappingFilePath, inputFilePath, delimiter = "\t", sc = spark.sparkContext)

    rdd.foreach(println(_))
    println(s"RDD LENGTH: ${rdd.count()}")
    assert(rdd.count() == 9)

    rdd.take(1)
      .foreach(triple =>
        assert(triple.getClass.getSimpleName == "Triple")
      )
  }

  "databus-client" should "convert tsv to ttl" in {

    val sc = spark.sparkContext

    val inputFilePath = s"${testDir}testBob.tsv"
    val mappingFilePath = s"${testDir}testBob.sparql"
    val outputFile= File(s"${testDir}testBob.ttl")
    val tempDir = File(s"${testDir}tempDir")
    if (tempDir.exists) tempDir.delete()

    val data = csv_map_to_rdd(mappingFilePath, inputFilePath, "\t", sc)

    RDF_Writer.convertToRDF(data, spark, RDFFormat.TURTLE_PRETTY).coalesce(1).saveAsTextFile(tempDir.pathAsString)
    FileUtil.unionFiles(tempDir, outputFile)

    println(s"number triples: ${data.count}")
    println(s"number lines outFile: ${outputFile.lineCount}")

    var subjects = Seq.empty[String]
    data.collect.foreach(triple=> subjects = subjects :+ triple.getSubject.toString)
    val countSubjects = subjects.distinct.length

    println(s"number triples outFile: ${(outputFile.lineCount - (countSubjects - 1))/2} ")
    assert(data.count() == (outputFile.lineCount - (countSubjects - 1))/2)
  }

  "databus-client" should "convert tsv to ttlTest" in {

    val sc = spark.sparkContext

    val inputFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-conversion-testbed_bob3.tsv"
    val mappingFilePath = "/home/eisenbahnplatte/git/databus-client/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-conversion-testbed_bob4_mapping.sparql"
    val outputFile= File("/home/eisenbahnplatte/git/databus-client/files/NoDataID/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-conversion-testbed_bob3.ttl")
    val tempDir = File(s"${testDir}tempDir")
    if (tempDir.exists) tempDir.delete()

    val data = csv_map_to_rdd(mappingFilePath, inputFilePath, "\t", sc)

    RDF_Writer.convertToRDF(data, spark, RDFFormat.TURTLE_PRETTY).coalesce(1).saveAsTextFile(tempDir.pathAsString)
    FileUtil.unionFiles(tempDir, outputFile)

    data.foreach(println(_))
    println(s"number triples: ${data.count}")
    println(s"number lines outFile: ${outputFile.lineCount}")

    var subjects = Seq.empty[String]
    data.collect.foreach(triple=> subjects = subjects :+ triple.getSubject.toString)
    val countSubjects = subjects.distinct.length

    println(s"number triples outFile: ${(outputFile.lineCount - (countSubjects - 1))/2} ")
    assert(data.count() == (outputFile.lineCount - (countSubjects - 1))/2)
  }

}
