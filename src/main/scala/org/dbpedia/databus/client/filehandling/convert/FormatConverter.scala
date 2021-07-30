package org.dbpedia.databus.client.filehandling.convert

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.convert.format.rdf.quads.QuadsHandler
import org.dbpedia.databus.client.filehandling.{CompileConfig, FileUtil}
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.mapping.{MappingInfo, RDF_Quads_Mapper, RDF_Triples_Mapper, TSD_Mapper}
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}

/**
 * Converter for tsv, csv and several RDF serializations (nt,ttl,rdfxml,json-ld, nq, trix, trig)
 */
object FormatConverter {

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

    val tempDir = File("./target/databus.tmp/temp/")
    if (tempDir.exists) tempDir.delete()

//    val targetFile: File = tempDir / file.nameWithoutExtension.concat(s".${conf.outputFormat}")

    if (RDF_TRIPLES.contains(conf.outputFormat)) { // convert to RDF_Triples
      val tripleHandler = new TripleHandler()

      //read process
      val triples = {
        if (RDF_TRIPLES.contains(conf.inputFormat)) tripleHandler.read(file.pathAsString, conf.inputFormat)
//        if (RDF_QUADS.contains(inputFormat)) RDF_Quads_Mapper.map_to_triples
        else TSD_Mapper.map_to_triples(file, conf)
      }

      //write process
      tripleHandler.write(triples, conf.outputFormat)
    }

    else if(RDF_QUADS.contains(conf.outputFormat)){ // convert to RDF_Quads
      val quadsHandler = new QuadsHandler()

      //read process
      val quads = {
        if (RDF_QUADS.contains(conf.inputFormat))  quadsHandler.read(file.pathAsString, conf.inputFormat)
        else RDF_Triples_Mapper.map_to_quads(new TripleHandler().read(file.pathAsString, conf.inputFormat))
      }

      //write process
      quadsHandler.write(quads, conf.outputFormat)
    }
    else { // convert to Tabular structured data (TSD)
      val tsdHandler = new TSDHandler(conf.delimiter)

      //read process
      val data = {
        if (TSD.contains(conf.inputFormat)) tsdHandler.read(file.pathAsString, conf.inputFormat)
//        else if (RDF_QUADS.contains(conf.inputFormat))  asd
        else { //RDF_TRIPLES.contains(conf.inputFormat)
          val triples = new TripleHandler().read(file.pathAsString, conf.inputFormat)
          RDF_Triples_Mapper.map_to_tsd(triples, conf.createMapping)
        }
      }

      //write
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
