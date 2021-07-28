package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.lang

import better.files.File
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.rdf.RDFLang

import java.io.ByteArrayOutputStream
import scala.io.{Codec, Source}

object RDFXML extends RDFLang[RDD[Triple]] {

  override def read(source:String)(implicit sc:SparkContext): RDD[Triple] = {

    val statements = RDFDataMgr.loadModel(source).listStatements()
    var data: Seq[Triple] = Seq.empty

    while (statements.hasNext) {
      data = data :+ statements.nextStatement().asTriple()
    }

    sc.parallelize(data)
  }

  override def write(data: RDD[Triple])(implicit sc:SparkContext): File = {
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2).collect()

    val os = new ByteArrayOutputStream()
    val models = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToRDF(allTriplesOfSubject)).toSeq

    val mergedModel: Model = ModelFactory.createDefaultModel()
    models.foreach(model => mergedModel.add(model))

    RDFDataMgr.write(os, mergedModel, Lang.RDFXML)

    val rdf_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "")

    sc.parallelize(Seq(rdf_string))
      .coalesce(1)
      .saveAsTextFile(tempDir.pathAsString)

    tempDir
  }

  def convertIteratorToRDF(triples: Iterable[Triple]): Model = {

    val model: Model = ModelFactory.createDefaultModel()

    triples.foreach(triple => {
      val stmt = ResourceFactory.createStatement(
        ResourceFactory.createResource(triple.getSubject.getURI),
        ResourceFactory.createProperty(triple.getPredicate.getURI),
        {
          if (triple.getObject.isLiteral) {
            if (triple.getObject.getLiteralLanguage.isEmpty) {
              ResourceFactory.createTypedLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralDatatype)
            }
            else ResourceFactory.createLangLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralLanguage)
          }
          else if (triple.getObject.isURI) ResourceFactory.createResource(triple.getObject.getURI)
          else model.asRDFNode(NodeFactory.createBlankNode())
        })
      model.add(stmt)
    })

    model
  }
}
