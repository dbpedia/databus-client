package org.dbpedia.databus.rdf_writer

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, RDFNode, ResourceFactory}
import org.apache.jena.riot.thrift.wire.RDF_IRI
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.shared.BadURIException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}

object RDFXML_Writer {

  def convertToRDFXML(data: RDD[Triple], spark: SparkSession): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val triplesRDFXML = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToRDFXML(allTriplesOfSubject))

    return triplesRDFXML
  }

  def convertIteratorToRDFXML(triples: Iterable[Triple]):String ={
    val model: Model = ModelFactory.createDefaultModel()
    val os = new ByteArrayOutputStream()

    triples.foreach(triple => {
      val stmt = ResourceFactory.createStatement(
        ResourceFactory.createResource(triple.getSubject.getURI),
        ResourceFactory.createProperty(triple.getPredicate.getURI),
        {
          println("hi")
          if(triple.getObject.isLiteral) {
            if(triple.getObject.getLiteralLanguage.isEmpty) ResourceFactory.createTypedLiteral(triple.getObject.getLiteralLexicalForm,triple.getObject.getLiteralDatatype)
            else ResourceFactory.createLangLiteral(triple.getObject.getLiteralLexicalForm,triple.getObject.getLiteralLanguage)
          }
          else if (triple.getObject.isURI) ResourceFactory.createResource(triple.getObject.getURI)
          else model.asRDFNode(NodeFactory.createBlankNode())
        })

      model.add(stmt)

//      val rdf_subject = model.createResource(triple.getSubject.getURI)
//      val rdf_predicate = model.createProperty(triple.getPredicate.getURI)
//      val rdf_object = {
//        if (triple.getObject.isLiteral) model.createTypedLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralDatatype)
//        else if (triple.getObject.isURI) model.createResource(triple.getObject.getURI)
//        else model.asRDFNode(NodeFactory.createBlankNode())
//      }
//      rdf_subject.addProperty(rdf_predicate, rdf_object)
    })

    RDFDataMgr.write(os, model, RDFFormat.RDFXML)

    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
  }
}
