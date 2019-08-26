package org.dbpedia.databus.rdf_writer

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}



object TTL_Writer {

  def convertToTTL(data: RDD[Triple], spark: SparkSession): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val triplesTTL = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToTTL(allTriplesOfSubject))

    return triplesTTL
  }

  def convertIteratorToTTL(triples: Iterable[Triple]):String ={
    val model: Model = ModelFactory.createDefaultModel()
    val os = new ByteArrayOutputStream()

    triples.foreach(triple => {
      val rdf_subject = model.createResource(triple.getSubject.toString())
      val rdf_object = model.createResource(triple.getObject.toString())
      val rdf_predicate = model.createProperty(triple.getPredicate.toString())
      rdf_subject.addProperty(rdf_predicate, rdf_object)
    })

    RDFDataMgr.write(os, model, RDFFormat.TURTLE)

    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
  }

}


