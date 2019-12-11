package org.dbpedia.databus.filehandling.convert.format.rdf.write

import java.io.ByteArrayOutputStream

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}

object JSONLD_Writer {

  def convertToJSONLD(data: RDD[Triple], spark: SparkSession): RDD[String] = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)

    triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToJSONLD(allTriplesOfSubject))
  }

  def convertIteratorToJSONLD(triples: Iterable[Triple]): String = {

    val os = new ByteArrayOutputStream()
    val model = ModelFactory.createDefaultModel()

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
    RDFDataMgr.write(os, model, RDFFormat.JSONLD_PRETTY)

    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
    //        if (Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().length <= 1) Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
    //        else s"""<script type="application/ld+json">\n""".concat(Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")).concat("</script>")
    //    val jsonld_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
  }

}
