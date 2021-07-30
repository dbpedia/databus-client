package org.dbpedia.databus.client.filehandling.convert

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Spark {

  val session:SparkSession = SparkSession.builder()
    .appName(s"Databus Client Converter")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val context: SparkContext = session.sparkContext
  context.setLogLevel("WARN")
}
