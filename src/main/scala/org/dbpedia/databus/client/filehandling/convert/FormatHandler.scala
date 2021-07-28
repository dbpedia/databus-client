package org.dbpedia.databus.client.filehandling.convert

import better.files.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.client.filehandling.FileUtil
import org.dbpedia.databus.client.filehandling.convert.format.tsd.TSDHandler
import org.dbpedia.databus.client.filehandling.convert.format.rdf.triples.TripleHandler
import org.dbpedia.databus.client.filehandling.convert.mapping.{MappingInfo, RDF_Quads_Mapper, RDF_Triples_Mapper, TSD_Mapper}
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}

/**
 * Converter for tsv, csv and several RDF serializations (nt,ttl,rdfxml,json-ld)
 */
object FormatHandler {

  var delimiter = ""
  var createMappingFile: Option[Boolean] = None

  /**
   * converts a file to a desired format
   *
   * @param inputFile    input file
   * @param inputFormat  input format
   * @param outputFormat output format
   * @param sha          sha256-sum of input file
   * @return converted file
   */
  def convertFormat(inputFile: File, inputFormat: String, outputFormat: String, sha: String, mapping: String, delimiter: Character, quotation: Character, createMapping: Boolean): File = {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    implicit val sparkContext: SparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val tempDir = File("./target/databus.tmp/temp/")
    if (tempDir.exists) tempDir.delete()

    val targetFile: File = tempDir / inputFile.nameWithoutExtension.concat(s".$outputFormat")
    var mappingFile = File("")

    if (EquivalenceClasses.RDF_TRIPLES.contains(outputFormat)) {

      //read
      val triples = {
        if (EquivalenceClasses.RDF_TRIPLES.contains(inputFormat)) {
          TripleHandler.read(inputFile.pathAsString, inputFormat)
        }
//        if (EquivalenceClasses.RDF_QUADS.contains(inputFormat)){
//          RDF_Quads_Mapper.map_to_triples
//        }
        else { // if (EquivalenceClasses.CSVTypes.contains(inputFormat)){
          TSD_Mapper.map_to_triples(spark, inputFile, inputFormat, sha, mapping, delimiter, quotation)
        }
      }

      //write
      TripleHandler.write(triples, outputFormat)
    }

    else if (EquivalenceClasses.TSD.contains(outputFormat)) {
      if (EquivalenceClasses.TSD.contains(inputFormat)) {
        val data: DataFrame = TSDHandler.read(inputFile.pathAsString, inputFormat, delimiter)
        TSDHandler.write(data, outputFormat, delimiter)
      }
      else {
        val triples = TripleHandler.read(inputFile.pathAsString, inputFormat)

        val newMappingFilePath = {
          if (createMapping) (tempDir / "mappingFile.sparql").pathAsString
          else ""
        }

        val dataFrame = RDF_Triples_Mapper.map_to_tsd(triples, spark, newMappingFilePath)
        TSDHandler.write(dataFrame, outputFormat, delimiter)

      }
    }

    try {
      FileUtil.unionFiles(tempDir, targetFile)
      if (mappingFile.exists && mappingFile != File("")) {
        val mapDir = File("./mappings/")
        mapDir.createDirectoryIfNotExists()
        mappingFile.moveTo(mapDir / FileUtil.getSha256(targetFile), overwrite = true)
      }
    }
    catch {
      case _: RuntimeException => LoggerFactory.getLogger("UnionFilesLogger").error(s"File $targetFile already exists") //deleteAndRestart(inputFile, inputFormat, outputFormat, targetFile: File)
    }

    targetFile
  }

}
