package org.dbpedia.databus.rdf_writer

import java.io.ByteArrayOutputStream

import net.sansa_stack.rdf.spark.io.NonSerializableObjectWrapper
import org.apache.jena.graph.{NodeFactory, Node_Variable, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}
import scala.reflect.ClassTag

//object JsonModelWrapper extends Serializable {
//  def createModel()= ModelFactory.createDefaultModel()
//  //  val model = ModelFactory.createDefaultModel()
//}
//
//class JsonModelWrapper(val model:Model) extends {
//
//}
//class JsonLDModelWrapper[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
//  def apply[T: ClassTag](constructor: => T): JsonLDModelWrapper[T] = new JsonLDModelWrapper[T](constructor)
//}
//
//object JsonLDModelWrapper {
//  def apply[T: ClassTag](constructor: => T): JsonLDModelWrapper[T] = new JsonLDModelWrapper[T](constructor)
//}
//private class NonSerializableObjectWrapper[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
//  @transient private lazy val instance: T = constructor
//
//  def get: T = instance
//}

object JSONLD_Writer {

  def convertToJSONLD(data: RDD[Triple], spark: SparkSession): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)

    val triplesJSONLD = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToJSONLD(allTriplesOfSubject))

    return triplesJSONLD
  }

  def convertIteratorToJSONLD(triples: Iterable[Triple]):String ={

    val os = new ByteArrayOutputStream()
    val model = ModelFactory.createDefaultModel()

    triples.foreach(triple => {
      val stmt = ResourceFactory.createStatement(
        ResourceFactory.createResource(triple.getSubject.getURI),
        ResourceFactory.createProperty(triple.getPredicate.getURI),
        {
          if(triple.getObject.isLiteral) {
            if(triple.getObject.getLiteralLanguage.isEmpty) ResourceFactory.createTypedLiteral(triple.getObject.getLiteralLexicalForm,triple.getObject.getLiteralDatatype)
            else ResourceFactory.createLangLiteral(triple.getObject.getLiteralLexicalForm,triple.getObject.getLiteralLanguage)
          }
          else if (triple.getObject.isURI) ResourceFactory.createResource(triple.getObject.getURI)
          else model.asRDFNode(NodeFactory.createBlankNode())
        })
      model.add(stmt)
    })
    RDFDataMgr.write(os, model, RDFFormat.JSONLD_PRETTY)

    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
//        if (Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().length <= 1) Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
//        else s"""<script type="application/ld+json">\n""".concat(Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")).concat("</script>")
//    val jsonld_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
  }

}
