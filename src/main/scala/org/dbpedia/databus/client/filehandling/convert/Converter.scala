package org.dbpedia.databus.client.filehandling.convert

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.filehandling.CompileConfig
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.mapping.{RDF_Quads_Mapper, RDF_Triples_Mapper, TSD_Mapper}

import java.net.URLEncoder

/**
 * Converter for tsv, csv and several RDF serializations (nt,ttl,rdfxml,json-ld, nq, trix, trig)
 */
object Converter {

  val RDF_TRIPLES: Seq[String] = Seq(
    "ttl",
    "nt",
    "rdfxml",
    "owl"
  )

  val RDF_QUADS: Seq[String] = Seq(
    "nq",
    "trix",
    "trig",
    "jsonld"
  )

  val TSD: Seq[String] = Seq(
    "tsv",
    "csv"
  )

  /**
   * converts a file to a desired format
   *
   * @param file    input file
   * @return converted file
   */
  def convert(file: File, conf:CompileConfig): File = {

    if (RDF_TRIPLES.contains(conf.outputFormat)) { // convert to RDF_Triples
      val tripleHandler = new TripleHandler()

      //read process
      if (RDF_QUADS.contains(conf.inputFormat)) {
        val targetTempDir = File("./target/databus.tmp/converted/")
        targetTempDir.createDirectoryIfNotExists()

        val quads = new QuadsHandler().read(file.pathAsString, conf.inputFormat)
        val triples = RDF_Quads_Mapper.map_to_triples(quads)

        triples.foreach(triplesResult => {
          val convertedFile = tripleHandler.write(triplesResult.graph, conf.outputFormat)
          val outFile = targetTempDir / s"${conf.outFile.nameWithoutExtension}_graph=${URLEncoder.encode(triplesResult.graphName, "UTF-8")}.${conf.outputFormat}"
          convertedFile.moveTo(outFile)
        })

        targetTempDir
      } else {
        val triples:Array[RDD[Triple]] = {
          if (RDF_TRIPLES.contains(conf.inputFormat)) {
            Array(tripleHandler.read(file.pathAsString, conf.inputFormat))
          }
          else { //TSD.contains(conf.inputFormat)
            Array(TSD_Mapper.map_to_triples(file, conf))
          }
        }

        tripleHandler.write(triples.head, conf.outputFormat)
      }

    }

    else if(RDF_QUADS.contains(conf.outputFormat)){ // convert to RDF_Quads
      val quadsHandler = new QuadsHandler()

      //read process
      val quads = {
        if (RDF_QUADS.contains(conf.inputFormat))  quadsHandler.read(file.pathAsString, conf.inputFormat)
        else if (RDF_TRIPLES.contains(conf.inputFormat)) RDF_Triples_Mapper.map_to_quads(new TripleHandler().read(file.pathAsString, conf.inputFormat), conf.graphURI)
        else Spark.context.emptyRDD[Quad]
      }

      //write process
      quadsHandler.write(quads, conf.outputFormat)
    }

    else { // convert to Tabular structured data (TSD)
      val tsdHandler = new TSDHandler(conf.delimiter)

      //read process
      val data = {
        if (TSD.contains(conf.inputFormat)) tsdHandler.read(file.pathAsString, conf.inputFormat)
        else if (RDF_QUADS.contains(conf.inputFormat)) {
          val quads = new QuadsHandler().read(file.pathAsString, conf.inputFormat)
          RDF_Quads_Mapper.map_to_tsd(quads, conf.createMapping)
        }
        else { //RDF_TRIPLES.contains(conf.inputFormat)
          val triples = new TripleHandler().read(file.pathAsString, conf.inputFormat)
          RDF_Triples_Mapper.map_to_tsd(triples, conf.createMapping)
        }
      }

      //write process
      tsdHandler.write(data, conf.outputFormat)
    }
  }
}
