package org.dbpedia.databus.filehandling.converter.mappings

import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.slf4j.LoggerFactory

object TSV_Reader {

  def csv_to_rddTriple(mapFile: String, csvFilePath: String = "", delimiter: Character = ',', quoteChar: Character = '"', sc: SparkContext): RDD[Triple] = {

    val tarqlQuery = new TarqlParser(mapFile).getResult

    val csvOptions = new CSVOptions()
    csvOptions.setDelimiter(delimiter)
    csvOptions.setQuoteChar(quoteChar)
//    {
//      if (delimiter == ',') CSVOptions.withCSVDefaults()
//      else if (delimiter == '\t') CSVOptions.withTSVDefaults()
//      else if (delimiter == ';') {
//        val csvOptions = new CSVOptions()
//        csvOptions.setDelimiter(CSVOptions.charNames.get("semicolon"))
//        csvOptions
//      }
//      else null
//    }

    println(s"\nUsed CSVOptions:\nEscapeCharacter: ${csvOptions.getEscapeChar}\nQuoteCharacter: ${csvOptions.getQuoteChar}\nEncoding: ${csvOptions.getEncoding}")

    var seq: Seq[Triple] = Seq.empty

    if (csvOptions != null) {
      val resultSet = csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlQuery).execTriples()
        case _ => TarqlQueryExecutionFactory.create(tarqlQuery, csvFilePath, csvOptions).execTriples()
      }

      while (resultSet.hasNext) seq = seq :+ resultSet.next()

    }
    else {
      LoggerFactory.getLogger("read_CSV").error(s"Delimiter: $delimiter not supported")
      println(s"ERROR (read_CSV): Delimiter: $delimiter not supported")
    }

    sc.parallelize(seq)
  }

  def csv_to_df(csvFilePath: String = "", delimiter: Character = ',', spark: SparkSession): DataFrame = {

    val data = spark.read.format("csv")
      .option("sep", delimiter.toString)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFilePath)

    data.foreach(println(_))

    data
  }


  //  def tsv_nt_map(spark: SparkSession): RDD[Triple] = {
  //
  //    val stream = new FileInputStream(File("/home/eisenbahnplatte/git/databus-client/test/bob3.tsv").toJava)
  //
  //    val mapping: java.util.Set[TriplesMap] =
  //      RmlMappingLoader
  //        .build()
  //        .load(RDFFormat.TURTLE, Paths.get("/home/eisenbahnplatte/git/databus-client/src/mapping/map_tsv_nt.ttl"))
  //
  //    val mapper =
  //      RmlMapper
  //        .newBuilder()
  //        .setLogicalSourceResolver(Rdf.Ql.Csv, new CsvResolver())
  //        .build()
  //    mapper.bindInputStream(stream)
  //
  //    val result: Model = mapper.map(mapping)
  //
  //    val sc = spark.sparkContext
  //    var data = sc.emptyRDD[Triple]
  //    val iter = result.iterator()
  //
  //    if (iter.hasNext) {
  //      val stmt: Statement = iter.next()
  //
  //      val subject: Resource = stmt.getSubject
  //      val pre: IRI = stmt.getPredicate
  //      val obj: Value = stmt.getObject
  //
  //      val triple = Triple.create(NodeFactory.createURI(stmt.getSubject.toString), NodeFactory.createURI(stmt.getPredicate.toString), NodeFactory.createLiteral(stmt.getObject.toString))
  //      data = sc.union(data, sc.parallelize(Seq(triple)))
  //    }
  //
  //    return sc.emptyRDD[Triple] //data
  //  }

}
