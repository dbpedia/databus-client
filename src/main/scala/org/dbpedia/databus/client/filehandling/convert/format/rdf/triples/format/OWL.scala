package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format

import better.files.File
import org.apache.commons.io.output.WriterOutputStream
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format
import org.semanticweb.owlapi.apibinding
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.RDFXMLDocumentFormatFactory
import org.semanticweb.owlapi.model.IRI
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream, InputStream, SequenceInputStream}
import java.nio.charset.Charset
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}

class OWL extends Format[RDD[Triple]]{

  val rdfxml = new RDFXML()

  override def read(source: String): RDD[Triple] = {

    val manager = OWLManager.createOWLOntologyManager
    val ontology = manager.loadOntologyFromOntologyDocument(File(source).toJava)

    println(ontology.getFormat)

    val it= ontology.getAnnotations.iterator()
    while(it.hasNext) println(it.next())
    val tempFile = tempDir.parent / "tempFile.rdf"
    val out = new FileOutputStream(tempFile.toJava)

    manager.saveOntology(
      ontology,
      new RDFXMLDocumentFormatFactory().createFormat(),
      out
    )

    println(tempFile.pathAsString)

    rdfxml.read(tempFile.pathAsString)
  }

   def write(data: RDD[Triple]): File ={

     val tempFile = rdfxml.write(data)

     val manager = OWLManager.createOWLOntologyManager
     val ontology = manager.loadOntologyFromOntologyDocument(tempFile.toJava)

     tempFile.delete()
     //     println(ontology.getAxiomCount())

     val outFile = tempDir / "converted.owl"

     val format = manager.getOntologyFormat(ontology)

     manager.saveOntology(ontology, format, IRI.create(outFile.uri))

     outFile
  }

}
