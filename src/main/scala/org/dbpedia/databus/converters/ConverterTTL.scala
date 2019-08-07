package org.dbpedia.databus.converters

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}

object ConverterTTL {

  def convertToJSONLD(data: RDD[Triple]): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val triplesJSONLD = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToJSONLD(allTriplesOfSubject))

    return triplesJSONLD
  }

  def convertIteratorToJSONLD(triples: Iterable[Triple]):String ={
    val model: Model = ModelFactory.createDefaultModel()
    val os = new ByteArrayOutputStream()

    triples.foreach(triple => {
      val rdf_subject = model.createResource(triple.getSubject.toString())
      val rdf_object = model.createResource(triple.getObject.toString())
      val rdf_predicate = model.createProperty(triple.getPredicate.toString())
      rdf_subject.addProperty(rdf_predicate, rdf_object)
    })

    RDFDataMgr.write(os, model, RDFFormat.TURTLE_PRETTY)

    val it = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines()
    var jsonString = ""
    it.foreach(part => jsonString = jsonString.concat(part))

    return jsonString
  }
}
