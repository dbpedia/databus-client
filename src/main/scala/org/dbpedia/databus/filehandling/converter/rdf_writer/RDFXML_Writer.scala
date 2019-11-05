package org.dbpedia.databus.filehandling.converter.rdf_writer

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}

object ModelWrapper extends Serializable {
  var model: Model = ModelFactory.createDefaultModel()

  def resetModel(): Unit = {
    model = ModelFactory.createDefaultModel()
  }
}

object RDFXML_Writer {

  def convertToRDFXML(data: RDD[Triple], spark: SparkSession): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)

    val os = new ByteArrayOutputStream()

    triplesGroupedBySubject.foreach(allTriplesOfSubject => convertIteratorToRDFXML(allTriplesOfSubject, ModelWrapper.model))

    RDFDataMgr.write(os, ModelWrapper.model, RDFFormat.RDFXML)
    val rdfxml_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
    ModelWrapper.resetModel()

    spark.sparkContext.parallelize(Seq(rdfxml_string))
  }

  def convertIteratorToRDFXML(triples: Iterable[Triple], model: Model): Unit = {

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
  }

}
