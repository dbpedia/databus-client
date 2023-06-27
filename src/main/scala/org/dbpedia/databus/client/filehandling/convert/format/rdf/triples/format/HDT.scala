package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format

import better.files.File
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.convert.format.Format
import org.apache.jena.graph.{NodeFactory, Triple}
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.jena.sparql.graph.GraphFactory
import org.dbpedia.databus.client.Config
import org.rdfhdt.hdt.enums.RDFNotation
import org.rdfhdt.hdt.hdt.{HDT => HDTFormat}
import org.rdfhdt.hdt.hdt.HDTManager
import org.rdfhdt.hdt.options.{HDTOptions, HDTSpecification}
import org.rdfhdt.hdt.triples.{TripleString, Triples}
import org.rdfhdt.hdtjena.HDTGraph

import collection.JavaConverters._

class HDT(baseURI:String) extends Format[RDD[Triple]]{

  override def read(source: String): RDD[Triple] = {

    // Load HDT file using the hdt-java library
    val hdt:HDTFormat = HDTManager.mapIndexedHDT(source, null)

    // Create Jena Model on top of HDT.
    val graph:HDTGraph = new HDTGraph(hdt)

    val model:Model = ModelFactory.createModelForGraph(graph)


    val stmtsSeq:Seq[Triple] = model.listStatements().toList.asScala.map(stmt=>stmt.asTriple()).toSeq
    Spark.context.parallelize(stmtsSeq)
  }

  override def write(data: RDD[Triple]): File = {

    val iterator:java.util.Iterator[TripleString] = data
      .collect()
      .map(triple =>
       {
         val tripleString = new TripleString(
           triple.getSubject.toString.asInstanceOf[CharSequence],
          triple.getPredicate.toString.asInstanceOf[CharSequence],
          triple.getObject.toString.asInstanceOf[CharSequence])

         tripleString
       }
    )
      .toIterator
      .asJava

    val hdt:HDTFormat = HDTManager.generateHDT(iterator, baseURI , new HDTSpecification(), null)

    val outFile = tempDir.parent / "tempFile.hdt"
    hdt.saveToHDT(outFile.newOutputStream, null)

    outFile
  }

}


