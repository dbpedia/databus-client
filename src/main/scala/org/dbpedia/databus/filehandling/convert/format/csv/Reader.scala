package org.dbpedia.databus.filehandling.convert.format.csv

import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.deri.tarql.{CSVOptions, TarqlParser, TarqlQueryExecutionFactory}
import org.slf4j.LoggerFactory

object Reader {

  def csv_to_rddTriple(mapFile: String, csvFilePath: String = "", delimiter: Character = ',', quoteChar: Character = '"', sc: SparkContext): RDD[Triple] = {

    val tarqlQuery = new TarqlParser(mapFile).getResult

    val csvOptions = new CSVOptions()
    csvOptions.setDelimiter(delimiter)
    csvOptions.setQuoteChar(quoteChar)

    println(
      s"""
         |Used CSVOptions:
         |Delimiter:      ${csvOptions.getDelimiter}
         |EscapeCharacter:${csvOptions.getEscapeChar}
         |QuoteCharacter: ${csvOptions.getQuoteChar}
         |Encoding:       ${csvOptions.getEncoding}
       """.stripMargin)

    var seq: Seq[Triple] = Seq.empty

    if (csvOptions != null) {
      val resultSet = csvFilePath match {
        case "" => TarqlQueryExecutionFactory.create(tarqlQuery).execTriples()
        case _ => TarqlQueryExecutionFactory.create(tarqlQuery, csvFilePath, csvOptions).execTriples()
      }

      import collection.JavaConverters._
      seq = resultSet.asScala.toSeq
    }
    else {
      LoggerFactory.getLogger("read_CSV").error(s"Delimiter: $delimiter not supported")
      println(s"ERROR (read_CSV): Delimiter: $delimiter not supported")
    }

    //seq.foreach(println(_))
    sc.parallelize(seq)
  }

  def csv_to_df(csvFilePath: String = "", delimiter: Character = ',', spark: SparkSession): DataFrame = {

    val data = spark.read.format("csv")
      .option("sep", delimiter.toString)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFilePath)

    data
  }

}
