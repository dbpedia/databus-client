package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.lang

import better.files.File
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.riot.lang.{PipedRDFIterator, PipedRDFStream, PipedTriplesStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayOutputStream
import java.util.concurrent.{ExecutorService, Executors}
import scala.io.{Codec, Source}

object Turtle {

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

    var data: Seq[Triple] = Seq.empty

    while (iter.hasNext) data = data :+ iter.next

    inputStream.finish()
    executor.shutdown()

    val sc = spark.sparkContext
    sc.parallelize(data)
  }

  def convertToTTL(data: RDD[Triple], spark: SparkSession): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val triplesTTL = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToTTL(allTriplesOfSubject))

    triplesTTL
  }

  def convertIteratorToTTL(triples: Iterable[Triple]): String = {
    val model: Model = ModelFactory.createDefaultModel()
    val os = new ByteArrayOutputStream()

    triples.foreach(triple => {
      val stmt = ResourceFactory.createStatement(
        ResourceFactory.createResource(triple.getSubject.getURI),
        ResourceFactory.createProperty(triple.getPredicate.getURI),
        {
          if (triple.getObject.isLiteral) {
            if (triple.getObject.getLiteralLanguage.isEmpty) ResourceFactory.createTypedLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralDatatype)
            else ResourceFactory.createLangLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralLanguage)
          }
          else if (triple.getObject.isURI) ResourceFactory.createResource(triple.getObject.getURI)
          else model.asRDFNode(NodeFactory.createBlankNode())
        })

      model.add(stmt)
    })

    RDFDataMgr.write(os, model, RDFFormat.TURTLE)

    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
  }
}
