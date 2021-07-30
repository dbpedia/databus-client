package org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.format

import better.files.File
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.jena.sparql.core.Quad
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.client.filehandling.convert.format.Format

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.io.{Codec, Source}

//class JsonLD extends Format[RDD[Quad]] {
//
////  def readJSONL(spark: SparkSession, inputFile: File): RDD[Triple] = {
////    val sc = spark.sparkContext
////    val data = sc.textFile(inputFile.pathAsString)
////    var tripleRDD = sc.emptyRDD[Triple]
////
////    //    data.foreach(println(_))
////
////    data.foreach(line => {
////      println(s"LINE: $line")
////      if (line.trim().nonEmpty) {
////        println(s"LINE: $line")
////        tripleRDD = sc.union(tripleRDD, readJSONLObject(spark, line))
////      }
////    })
////
////    tripleRDD
////  }
////
////  def readJSONLObject(spark: SparkSession, line: String): RDD[Triple] = {
////    //    println(line)
////    val sc = spark.sparkContext
////    var triples = sc.emptyRDD[Triple]
////
////    val statements = ModelFactory.createDefaultModel().read(new ByteArrayInputStream(line.getBytes), "UTF-8", "JSONLD").listStatements()
////
////    while (statements.hasNext) {
////      val triple = statements.nextStatement().asTriple()
////      val dataTriple = sc.parallelize(Seq(triple))
////      triples = sc.union(triples, dataTriple)
////    }
////
////    triples
////  }
////
////  def convertIteratorToJSONLD(triples: Iterable[Triple]): String = {
////
////    val os = new ByteArrayOutputStream()
////    val model = ModelFactory.createDefaultModel()
////
////    triples.foreach(triple => {
////      val stmt = ResourceFactory.createStatement(
////        ResourceFactory.createResource(triple.getSubject.getURI),
////        ResourceFactory.createProperty(triple.getPredicate.getURI),
////        {
////          if (triple.getObject.isLiteral) {
////            if (triple.getObject.getLiteralLanguage.isEmpty) ResourceFactory.createTypedLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralDatatype)
////            else ResourceFactory.createLangLiteral(triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralLanguage)
////          }
////          else if (triple.getObject.isURI) ResourceFactory.createResource(triple.getObject.getURI)
////          else model.asRDFNode(NodeFactory.createBlankNode())
////        })
////      model.add(stmt)
////    })
////    RDFDataMgr.write(os, model, RDFFormat.JSONLD_PRETTY)
////
////    Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
////    //        if (Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().length <= 1) Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
////    //        else s"""<script type="application/ld+json">\n""".concat(Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")).concat("</script>")
////    //    val jsonld_string = Source.fromBytes(os.toByteArray)(Codec.UTF8).getLines().mkString("", "\n", "\n")
////  }
////
////  def convertToJSONLD(data: RDD[Triple], spark: SparkSession): RDD[String] = {
////    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
////
////    triplesGroupedBySubject.map(allTriplesOfSubject => JsonLD.convertIteratorToJSONLD(allTriplesOfSubject))
////  }
//
//  override def read(source: String)(implicit sc: SparkContext): RDD[Quad] = ???
//
//  override def write(data: RDD[Quad])(implicit sc: SparkContext): File = ???
//}
