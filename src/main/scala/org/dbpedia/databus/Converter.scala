package org.dbpedia.databus


import java.io.{BufferedInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.file.NoSuchFileException

import scala.language.postfixOps
import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.rdd.RDD
import org.dbpedia.databus.converters.{ConverterJSONLD, ConverterTSV, ConverterTTL}
import org.slf4j.LoggerFactory
import org.apache.jena.graph.Triple


object Converter {

  def decompress(bufferedInputStream: BufferedInputStream): InputStream = {
    try {
      val compressorIn: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
      return compressorIn
    }
    catch {
      case noCompression: CompressorException => return bufferedInputStream
      case inInitializerError: ExceptionInInitializerError => return bufferedInputStream
      case noClassDefFoundError: NoClassDefFoundError => return bufferedInputStream
    }
  }

  def convertFormat(inputFile: File, inputFormat:String, outputFormat: String): File = {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val data = inputFormat match {
      case "nt" | "ttl" | "rdf" => readTriplesWithSansa(spark,inputFile)
      case "jsonld" => readTriplesJSONLD(spark, inputFile)
    }


    val tempDir = s"${inputFile.parent.pathAsString}/temp"
    val headerTempDir = s"${inputFile.parent.pathAsString}/tempheader"

    val targetFile: File = File(tempDir) / inputFile.nameWithoutExtension.concat(s".$outputFormat")

    //delete temp directory if exists
    try {
      File(tempDir).delete()
    } catch {
      case noFile: NoSuchFileException => ""
    }


    outputFormat match {
      case "nt" => data.saveAsNTriplesFile(tempDir)
      case "tsv" => {
        val solution = ConverterTSV.convertToTSV(data, spark)
        solution(1).write.option("delimiter", "\t").csv(tempDir)
        solution(0).write.option("delimiter", "\t").csv(headerTempDir)
      }
      case "ttl" => ConverterTTL.convertToJSONLD(data).saveAsTextFile(tempDir)
      case "jsonld" => ConverterJSONLD.convertToJSONLD(data).saveAsTextFile(tempDir)
    }

    try {
      outputFormat match {
        case "tsv" => FileHandler.unionFilesWithHeaderFile(headerTempDir, tempDir, targetFile)
        case "jsonld" | "nt" | "ttl" => FileHandler.unionFiles(tempDir, targetFile)
      }
    }
    catch {
      case fileAlreadyExists: RuntimeException => deleteAndRestart(inputFile ,inputFormat, outputFormat, targetFile: File)
    }


    return targetFile
  }


  def readTriplesWithSansa(spark: SparkSession, inputFile:File): RDD[Triple] = {
    val logger = LoggerFactory.getLogger("ErrorlogReadTriples")
    NTripleReader.load(spark, inputFile.pathAsString, ErrorParseMode.SKIP, WarningParseMode.IGNORE, false, logger)
  }

  def readTriplesJSONLD(spark: SparkSession, inputFile:File): RDD[Triple] = {
    val logger = LoggerFactory.getLogger("ErrorlogReadTriples")
    NTripleReader.load(spark, inputFile.pathAsString, ErrorParseMode.SKIP, WarningParseMode.IGNORE, false, logger)
  }

  def deleteAndRestart(inputFile: File, inputFormat:String, outputFormat: String, file: File): Unit = {
    file.delete()
    convertFormat(inputFile, inputFormat, outputFormat)
  }

  def compress(outputCompression: String, output: File): OutputStream = {
    try {
      // file is created here
      val myOutputStream = new FileOutputStream(output.toJava)
      val out: OutputStream = outputCompression match {
        case "bz2" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, myOutputStream)
        case "gz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream)
        case "br" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BROTLI, myOutputStream)
        case "deflate" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE, myOutputStream)
        case "deflate64" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE64, myOutputStream)
        case "lz4-block" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_BLOCK, myOutputStream)
        case "lz4-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_FRAMED, myOutputStream)
        case "lzma" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZMA, myOutputStream)
        case "pack200" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.PACK200, myOutputStream)
        case "snappy-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, myOutputStream)
        case "snappy-raw" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_RAW, myOutputStream)
        case "xz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.XZ, myOutputStream)
        case "z" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.Z, myOutputStream)
        case "zstd" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, myOutputStream)
        case "" => myOutputStream //if outputCompression is empty
      }
      return out
    }
  }

}








//  def convertToTSV(data: RDD[Triple], spark: SparkSession): RDD[String]={
//
//    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
//    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
//    val allPredicates = data.groupBy(triple => triple.getPredicate.toString()).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?
//
//    val allPredicateVector = allPredicates.collect.toVector
//
//    val triplesTSV = triplesGroupedBySubject.map(allTriplesOfSubject => convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, allPredicateVector))
//
//    val headerString = "resource\t".concat(allPredicateVector.mkString("\t"))
//    val header = spark.sparkContext.parallelize(Seq(headerString))
//
//    val TSV_RDD = header ++ triplesTSV
//
//    TSV_RDD.sortBy(_(1), ascending = false)
//
//    return TSV_RDD
//  }
//
//  def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], allPredicates: Vector[String]): String={
//    var TSVstr = triples.last.getSubject.toString
//
//    allPredicates.foreach(predicate => {
//      var alreadyIncluded=false
//      var tripleObject = ""
//
//      triples.foreach(z => {
//        val triplePredicate = z.getPredicate.toString()
//        if(predicate == triplePredicate) {
//          alreadyIncluded = true
//          tripleObject = z.getObject.toString()
//        }
//      })
//
//      if(alreadyIncluded == true) {
//        TSVstr = TSVstr.concat(s"\t$tripleObject")
//      }
//      else{
//        TSVstr = TSVstr.concat("\t")
//      }
//    })
//
//    return TSVstr
//  }

//  def convertToJSONLD(data: RDD[Triple]): RDD[String] = {
//    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
//    val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
//    val JSONTriples = triplesGroupedBySubject.map(allTriplesOfSubject => convertIterableToJSONLD(allTriplesOfSubject))
//
//
//    return JSONTriples
//  }



//  def convertIterableToJSONLD(triples: Iterable[Triple]): String = {
//
//    var JSONstr =s"""{"@id": "${triples.last.getSubject.toString}","""
//    var vectorTriples = Vector(Vector[String]())
//
//    triples.foreach(triple => {
//      val predicate = triple.getPredicate.toString()
//      val obj = triple.getObject.toString()
//      var alreadyContainsPredicate = false
//
//      var i = 0
//      vectorTriples.foreach(vector => {
//        if (vector.contains(predicate)) {
//          alreadyContainsPredicate = true
//          vectorTriples = vectorTriples.updated(i, vector :+ obj)
//        }
//        i += 1
//      })
//
//      if (alreadyContainsPredicate == false) {
//        vectorTriples = vectorTriples :+ Vector(predicate, obj)
//      }
//    })
//
//    //    println("VEKTOR")
//    //    vectorTriples.foreach(vect=> {
//    //      vect.foreach(println(_))
//    //      println("\n")
//    //    })
//
//    vectorTriples.foreach(vector => {
//      if (vector.nonEmpty) {
//        JSONstr = JSONstr.concat(s""""${vector(0)}": """) // PRAEDIKAT WIRD OHNE KLAMMERN HINZUGEFUEGT
//        var k = 0
//        vector.foreach(element => {
//          if (k > 0) { // OBJEKTE
//            if (element.contains("^^")) {
//              val split = element.split("\\^\\^")
//              JSONstr = JSONstr.concat(s"""[{"@value":${split(0)},""")
//              JSONstr = JSONstr.concat(s""""@type":"${split(1)}"}],""")
//            } else {
//              JSONstr = JSONstr.concat(s"""["$element"],""")
//            }
//          }
//          k += 1
//        })
//      }
//    })
//    JSONstr = JSONstr.dropRight(1).concat("},")
//    return JSONstr
//  }