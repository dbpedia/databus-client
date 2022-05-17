package org.dbpedia.databus.client.filehandling.convert

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.client.Config
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.mapping.{RDF_Quads_Mapper, RDF_Triples_Mapper, TSD_Mapper}

import java.net.URLEncoder

/**
 * Converter for tsv, csv and several RDF serializations (nt,ttl,rdfxml,json-ld, nq, trix, trig)
 */
object Converter {

  val RDF_TRIPLES: Seq[String] = Config.RDF_TRIPLES
  val RDF_QUADS: Seq[String] = Config.RDF_QUADS
  val TSD: Seq[String] = Config.TSD

  /**
   * converts a file to a desired format
   *
   * @param file    input file
   * @return converted file
   */
  def convert(file: File, conf:ConvertConfig): File = {

    if (RDF_TRIPLES.contains(conf.outFormat)) { // convert to RDF_Triples
      val tripleHandler = new TripleHandler()

      //read process
      if (RDF_QUADS.contains(conf.inFormat)) {
        val targetTempDir = File("./target/databus.tmp/converted/")
        targetTempDir.createDirectoryIfNotExists()

        val quads = new QuadsHandler().read(file.pathAsString, conf.inFormat)
        val triples = RDF_Quads_Mapper.map_to_triples(quads)

        triples.foreach(triplesResult => {
          val convertedFile = tripleHandler.write(triplesResult.graph, conf.outFormat, conf.baseURI)
          val outFile = targetTempDir / s"${conf.outFile.nameWithoutExtension}_graph=${URLEncoder.encode(triplesResult.graphName, "UTF-8")}.${conf.outFormat}"
          convertedFile.moveTo(outFile)
        })

        targetTempDir
      } else {
        val triples:Array[RDD[Triple]] = {
          if (RDF_TRIPLES.contains(conf.inFormat)) {
            Array(tripleHandler.read(file.pathAsString, conf.inFormat))
          }
          else { //TSD.contains(conf.inputFormat)
            Array(TSD_Mapper.map_to_triples(file, conf))
          }
        }

        tripleHandler.write(triples.head, conf.outFormat, conf.baseURI)
      }

    }

    else if(RDF_QUADS.contains(conf.outFormat)){ // convert to RDF_Quads
      val quadsHandler = new QuadsHandler()

      //read process
      val quads = {
        if (RDF_QUADS.contains(conf.inFormat))  quadsHandler.read(file.pathAsString, conf.inFormat)
        else if (RDF_TRIPLES.contains(conf.inFormat)) RDF_Triples_Mapper.map_to_quads(new TripleHandler().read(file.pathAsString, conf.inFormat), conf.graphURI)
        else Spark.context.emptyRDD[Quad]
      }

      //write process
      quadsHandler.write(quads, conf.outFormat)
    }

    else { // convert to Tabular structured data (TSD)
      val tsdHandler = new TSDHandler(conf.delimiter)

      //read process
      val data = {
        if (TSD.contains(conf.inFormat)) tsdHandler.read(file.pathAsString, conf.inFormat)
        else if (RDF_QUADS.contains(conf.inFormat)) {
          val quads = new QuadsHandler().read(file.pathAsString, conf.inFormat)
          RDF_Quads_Mapper.map_to_tsd(quads, conf.createMapping)
        }
        else { //RDF_TRIPLES.contains(conf.inputFormat)
          val triples = new TripleHandler().read(file.pathAsString, conf.inFormat)
          RDF_Triples_Mapper.map_to_tsd(triples, conf.createMapping)
        }
      }

      //write process
      tsdHandler.write(data, conf.outFormat)
    }
  }
}
