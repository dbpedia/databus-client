package org.dbpedia.databus.rdf_writer

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
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
      val stmt = ResourceFactory.createStatement(
        ResourceFactory.createResource(triple.getSubject.getURI),
        ResourceFactory.createProperty(triple.getPredicate.getURI),
        {
          if(triple.getObject.isLiteral) ResourceFactory.createTypedLiteral(triple.getObject.getLiteralLexicalForm,triple.getObject.getLiteralDatatype)
          else if (triple.getObject.isURI) ResourceFactory.createResource(triple.getObject.getURI)
          else model.asRDFNode(NodeFactory.createBlankNode())
        })

      model.add(stmt)
    })

    RDFDataMgr.write(os, model, RDFFormat.TURTLE)

    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
  }

}

