package org.dbpedia.databus.filehandling.converter.rdf_reader

import java.util.concurrent.{ExecutorService, Executors}

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.{PipedRDFIterator, PipedRDFStream, PipedTriplesStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TTL_Reader {

  def read(spark: SparkSession, inputFile: File): RDD[Triple] = {
    // Create a PipedRDFStream to accept input and a PipedRDFIterator to consume it
    // You can optionally supply a buffer size here for the
    // PipedRDFIterator, see the documentation for details about recommended buffer sizes
    val iter: PipedRDFIterator[Triple] = new PipedRDFIterator[Triple]()
    val inputStream: PipedRDFStream[Triple] = new PipedTriplesStream(iter)

    // PipedRDFStream and PipedRDFIterator need to be on different threads
    val executor: ExecutorService = Executors.newSingleThreadExecutor()

    // Create a runnable for our parser thread
    val parser: Runnable = new Runnable() {

      @Override
      def run(): Unit = {
        // Call the parsing process.
        RDFDataMgr.parse(inputStream, inputFile.pathAsString)
      }
    }

    // Start the parser on another thread
    executor.submit(parser)

    // We will consume the input on the main thread here

    // We can now iterate over data as it is parsed, parsing only runs as
    // far ahead of our consumption as the buffer size allows

    var data:Seq[Triple] = Seq.empty

    while (iter.hasNext) data = data :+ iter.next

    inputStream.finish()
    executor.shutdown()

    val sc = spark.sparkContext
    sc.parallelize(data)
  }
}
