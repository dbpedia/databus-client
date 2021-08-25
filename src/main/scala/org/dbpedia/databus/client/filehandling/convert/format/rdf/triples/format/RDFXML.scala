package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format

import better.files.File
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format

import java.io.ByteArrayOutputStream
import scala.io.{Codec, Source}

class RDFXML extends Format[RDD[Triple]] {

  override def read(source:String): RDD[Triple] = {

    val statements = RDFDataMgr.loadModel(source).listStatements()
    var data: Seq[Triple] = Seq.empty

    while (statements.hasNext) {
      data = data :+ statements.nextStatement().asTriple()
    }

    Spark.context.parallelize(data)
  }

  override def write(data: RDD[Triple]): File = {
    convertToRDF(data)
  }

  def convertToRDF(data: RDD[Triple], lang:Lang = Lang.RDFXML)={
    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2).collect()

    val os = new ByteArrayOutputStream()
    val models = triplesGroupedBySubject.map(allTriplesOfSubject => convertIteratorToRDF(allTriplesOfSubject)).toSeq

    val mergedModel: Model = ModelFactory.createDefaultModel()
    models.foreach(model => mergedModel.add(model))

    RDFDataMgr.write(os, mergedModel, lang)

    val rdf_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "")

    Spark.context.parallelize(Seq(rdf_string))
      .coalesce(1)
      .saveAsTextFile(tempDir.pathAsString)

    FileUtil.unionFiles(tempDir, tempDir / "converted")
  }

  def convertIteratorToRDF(triples: Iterable[Triple]): Model = {

    val model: Model = ModelFactory.createDefaultModel()

    triples.foreach(triple => {
      val stmt = ResourceFactory.createStatement(
        if(triple.getSubject.isBlank) ResourceFactory.createResource()
        else ResourceFactory.createResource(triple.getSubject.getURI),
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
