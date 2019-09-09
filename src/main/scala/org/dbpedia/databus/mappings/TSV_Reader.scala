package org.dbpedia.databus.mappings

import java.io.FileInputStream
import java.nio.file.Paths

import better.files
import better.files.File
import com.taxonic.carml.engine.RmlMapper
import com.taxonic.carml.logical_source_resolver.CsvResolver
import com.taxonic.carml.model.TriplesMap
import com.taxonic.carml.util.RmlMappingLoader
import com.taxonic.carml.vocab.Rdf
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.eclipse.rdf4j.model.{IRI, Model, Resource, Statement, Value}
import org.eclipse.rdf4j.rio.RDFFormat

import collection.JavaConverters._

object TSV_Reader {

//  def map(inputFile:File, outputFile:File, mappingFile:File) ={
//    val in = new FileOrURISource(inputFile.pathAsString)
//
//    // Output to local file in RDF/XML format
//    //		Output out = new RDFXMLOutput("example2_output.xml");
//    val out = new NTriplesOutput(outputFile.pathAsString)
//
//    // Create an in-memory repository from a local file
//    val mappingRepository = Repository.createFileOrUriRepository(mappingFile.pathAsString)
//
//    // Specify target dataset. Just generate any statement containing one of the properties
//    val vocabulary = """@prefix foaf: <http://xmlns.com/foaf/0.1/> .
//      @prefix dbpedia: <http://dbpedia.org/ontology/> .
//      @prefix v: <http://www.w3.org/2006/vcard/ns#> .
//      (
//      foaf:mbox,
//      dbpedia:birthDay,
//      v:n
//      )"""
//
//    // Transform: The output data is written to LabelToName_Output.nt
//    Mapper.transform(in, out, mappingRepository, vocabulary)
//
//    // Close the Output object to write the data to file
//    out.close()
//  }

  def tsv_nt_map(spark: SparkSession):RDD[Triple]= {

    val stream = new FileInputStream(File("/home/eisenbahnplatte/git/databus-client/test/bob3.tsv").toJava)

    val mapping: java.util.Set[TriplesMap] =
      RmlMappingLoader
        .build()
        .load(RDFFormat.TURTLE, Paths.get("/home/eisenbahnplatte/git/databus-client/src/mapping/map_tsv_nt.ttl"))

    val mapper =
      RmlMapper
        .newBuilder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, new CsvResolver())
        .build()
    mapper.bindInputStream(stream)

    val result: Model = mapper.map(mapping)

    val sc =spark.sparkContext
    var data = sc.emptyRDD[Triple]
    val iter = result.iterator()

    if (iter.hasNext) {
      val stmt: Statement= iter.next()
      println(stmt)

      val subject: Resource = stmt.getSubject
      val pre: IRI = stmt.getPredicate
      val obj: Value = stmt.getObject

      val triple =Triple.create(NodeFactory.createURI(stmt.getSubject.toString),NodeFactory.createURI(stmt.getPredicate.toString),NodeFactory.createLiteral(stmt.getObject.toString))
      data = sc.union(data, sc.parallelize(Seq(triple)))
    }

    return sc.emptyRDD[Triple]//data
  }

}
