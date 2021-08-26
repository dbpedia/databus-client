package org.dbpedia.databus.client.filehandling.convert

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.{CompileConfig, FileUtil}
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.mapping.{RDF_Quads_Mapper, RDF_Triples_Mapper, TSD_Mapper}
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD

import java.net.URLEncoder
/**
 * Converter for tsv, csv and several RDF serializations (nt,ttl,rdfxml,json-ld, nq, trix, trig)
 */
object FormatConverter {
  val tempDir = File("./target/databus.tmp/temp/")
  val targetTempDir = File("./target/databus.tmp/converted/")

  val RDF_TRIPLES: Seq[String] = Seq(
    "ttl",
    "nt",
    "rdfxml",
    "jsonld"
  )

  val RDF_QUADS: Seq[String] = Seq(
    "nq",
    "trix",
    "trig"
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

    if (tempDir.exists) tempDir.delete()
    if (targetTempDir.exists) targetTempDir.clear()
    else targetTempDir.createDirectory()

    if (RDF_TRIPLES.contains(conf.outputFormat)) { // convert to RDF_Triples
      val tripleHandler = new TripleHandler()

      //read process
      if (RDF_QUADS.contains(conf.inputFormat)) {
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
        else RDF_Triples_Mapper.map_to_quads(new TripleHandler().read(file.pathAsString, conf.inputFormat), conf.graphURI)
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

      tsdHandler.write(data, conf.outputFormat)
    }
  }
//  FileUtil.unionFiles(tempDir, targetFile)
//  if (mappingFile.exists && mappingFile != File("")) {
//    val mapDir = File("./mappings/")
//    mapDir.createDirectoryIfNotExists()
//    mappingFile.moveTo(mapDir / FileUtil.getSha256(targetFile), overwrite = true)
//  }
//}
//catch {
//  case _: RuntimeException => LoggerFactory.getLogger("UnionFilesLogger").error(s"File $targetFile already exists") //deleteAndRestart(inputFile, inputFormat, outputFormat, targetFile: File)
//}
//
//  targetFile
}
