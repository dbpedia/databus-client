package org.dbpedia.databus.rdf_writer

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.{NodeFactory, Node_Variable, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}

object JSONLD_Writer {

  def convertToJSONLD(data: RDD[Triple]): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)

    val triplesJSONLD = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToJSONLD(allTriplesOfSubject))

    return triplesJSONLD
  }

  def convertIteratorToJSONLD(triples: Iterable[Triple]):String ={
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

    RDFDataMgr.write(os, model, RDFFormat.JSONLD_PRETTY)


    if (Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().length <= 1) Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
    else s"""<script type="application/ld+json">\n""".concat(Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")).concat("</script>")
//    var jsonString = ""
//    it.foreach(part => jsonString = jsonString.concat(part))
//
//    return jsonString
  }

}
