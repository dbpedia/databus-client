package org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.format

import better.files.File
import org.apache.commons.io.output.WriterOutputStream
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.Spark
import org.dbpedia.databus.client.filehandling.convert.format.Format
import org.semanticweb.owlapi.{apibinding, formats}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.{FunctionalSyntaxDocumentFormat, ManchesterSyntaxDocumentFormat, OWLXMLDocumentFormat, PrefixDocumentFormat, RDFXMLDocumentFormatFactory, TurtleDocumentFormat}
import org.semanticweb.owlapi.io.SystemOutDocumentTarget
import org.semanticweb.owlapi.model.{IRI, OWLDocumentFormat, OWLOntologyCreationException, OWLOntologyStorageException}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream, InputStream, SequenceInputStream}
import java.nio.charset.Charset
import scala.collection.JavaConverters.{asJavaEnumerationConverter, asJavaIteratorConverter, asScalaIteratorConverter}

class OWL(outputFormat:PrefixDocumentFormat=new FunctionalSyntaxDocumentFormat()) extends Format[RDD[Triple]]{

  val manager = OWLManager.createOWLOntologyManager
  val ntripleHandler = new NTriples()
  val turtleHandler = new Turtle()

  override def read(source: String): RDD[Triple] = {

    val manager = OWLManager.createOWLOntologyManager
    val ontology = manager.loadOntologyFromOntologyDocument(File(source).toJava)

    val tempFile = tempDir.parent / "tempFile.ttl"

    manager.saveOntology(ontology, new TurtleDocumentFormat(), tempFile.newOutputStream)

    tempFile.deleteOnExit()

    turtleHandler.read(tempFile.pathAsString)
  }

   def write(data: RDD[Triple]): File ={

     val extension = outputFormat match {
       case manchester:ManchesterSyntaxDocumentFormat => ".omn"
       case owlxml:OWLXMLDocumentFormat => ".owx"
       case _ => ".owl"
     }

     val outFile = tempDir / "converted".concat(extension)

     try {
       val tempFile = ntripleHandler.write(data)
       val ontology = manager.loadOntologyFromOntologyDocument(tempFile.toJava)

       tempFile.delete()

       val inputFormat:OWLDocumentFormat = manager.getOntologyFormat(ontology)

       if (inputFormat.isPrefixOWLDocumentFormat()) {
         outputFormat.copyPrefixesFrom(inputFormat.asPrefixOWLDocumentFormat());
       }
       manager.saveOntology(ontology, outputFormat, outFile.newOutputStream);
     } catch {
       case owlOntologyCreationException:OWLOntologyCreationException =>
         logger.error("Could not load ontology: " + owlOntologyCreationException.getMessage())
       case owlOntologyStorageException:OWLOntologyStorageException =>
         logger.error("Could not save ontology: " + owlOntologyStorageException.getMessage())
     }

     outFile
  }

}
