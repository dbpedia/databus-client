package org.dbpedia.databus

import java.io.{BufferedInputStream, BufferedWriter, FileOutputStream, FileWriter, InputStream, OutputStream}

import scala.sys.process._
import scala.language.postfixOps
import scala.util.control.Breaks._
import scala.io.Source
import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Vector
import scala.collection.mutable.ListBuffer


object Converter {

  def getCompressionType(fileInputStream: BufferedInputStream): String ={

    try{
      var ctype = CompressorStreamFactory.detect(fileInputStream)
      if (ctype == "bzip2"){
        ctype="bz2"
      }
      return ctype
    }
    catch{
      case noCompression: CompressorException => ""
    }
  }

  def getFormatType(inputFile: File): String ={
    // Suche in Dataid.ttl nach allen Zeilen die den Namen der Datei enthalten
    val lines = Source.fromFile((inputFile.parent / "dataid.ttl").pathAsString).getLines().filter(_ contains s"${inputFile.name}")

    val regex = s"<\\S*dataid.ttl#${inputFile.name}\\S*>".r
    var fileURL = ""

    for(line<-lines){
      breakable {
        for(x<-regex.findAllMatchIn(line)) {
          fileURL = x.toString().replace(">","").replace("<","")
          break
        }
      }
    }

    val fileType = QueryHandler.getTypeOfFile(fileURL, inputFile.parent / "dataid.ttl")
    return fileType
  }

  def decompress(bufferedInputStream: BufferedInputStream): InputStream = {
    try {
      val compressorIn: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
      return compressorIn
    }
    catch{
      case noCompression: CompressorException => bufferedInputStream
    }
  }

//  def handleNoCompressorException(fileInputStream: FileInputStream): BufferedInputStream ={
//    new BufferedInputStream(fileInputStream)
//  }

  def convertFormat(inputFile: File, outputFormat:String): File= {

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    println(inputFile.pathAsString)

    val data = NTripleReader.load(spark, inputFile.pathAsString)
    val targetFile:File = inputFile.parent / inputFile.nameWithoutExtension.concat(s".$outputFormat")

    val tempDir = s"${inputFile.parent.pathAsString}/temp"


    if (outputFormat=="nt"){
      data.saveAsNTriplesFile(tempDir)
    }
    else{
      //Erstelle Vector von allen Tripeln
//      var triples: Vector[Triple] = Vector.empty
//      tripleIterators.foreach(tripleIter => tripleIter.foreach(triple => triples = triples :+ triple))
//      //triples.foreach(println(_))
//
      val convertedTriples = outputFormat match {
        case "tsv" => convertToTSV(data, spark)
//        case "jsonld" => convertToJSONLD(data, spark)
      }

      val tempDir2="test/temp"
      val targetFile2=File("test/result")

      convertedTriples.saveAsTextFile(tempDir)
    }

    try {

      val findTripleFiles = s"find $tempDir/ -name part*" !!
      val concatFiles = s"cat $findTripleFiles" #> targetFile.toJava !

      if( concatFiles == 0 ) FileUtils.deleteDirectory(File(tempDir).toJava)
      else System.err.println(s"[WARN] failed to merge $tempDir/*")
    }
    catch {
      case fileAlreadyExists: RuntimeException => deleteAndRestart(inputFile: File, outputFormat: String, targetFile: File)
    }

    return targetFile
  }

//  def convertTriplesToTSV(tripleIters: RDD[Iterable[Triple]]): RDD[String] ={
//    val convertedTriples:RDD[String]= tripleIters.map(iterable => convertIter(iterable))
////ripleIters.map(y => y.map(z => s"${z.getSubject}\t${z.getObject}\t${z.getPredicate}"))
//    return convertedTriples
//  }

  def convertToTSV(data: RDD[Triple], spark: SparkSession): RDD[String]={

    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
    val tripleIterators = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
    val allPredicates = data.groupBy(triple => triple.getPredicate.toString()).map(_._1)
    //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?

    var predicateVector = allPredicates.collect.toVector
    predicateVector.foreach(println(_))

    val predicates = "resource\t".concat(predicateVector.mkString("\t"))

    println(s"PREDICATES: $predicates")

    val triplesRDD = tripleIterators.map(iterable => convertIterableToTSV(iterable, predicateVector))

    val header = spark.sparkContext.parallelize(Seq(predicates))
    val TSVTriples = header ++ triplesRDD
    TSVTriples.sortBy(_(1), ascending = false)

    return TSVTriples
  }

  def convertIterableToTSV(iter :Iterable[Triple], allPredicates: Vector[String]): String={
    var str=iter.last.getSubject.toString

    allPredicates.foreach(predicate => {
      var include=false
      var tripleObject = ""

      iter.foreach(z => {
        val triplePredicate = z.getPredicate.toString()
        if(predicate == triplePredicate) {
          include = true
          tripleObject = z.getObject.toString()
        }
      })

      if(include == true) {
        str = str.concat(s"\t$tripleObject")
      }
      else{
        str = str.concat("\t")
      }
    })

    return str
  }

//  def convertToJSONLD(data: RDD[Triple]): RDD[String]={
//    //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
//    val tripleIterators = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
//
//    val triplesRDD = tripleIterators.map(iterable => convertIterableToJSONLD(iterable))
//
//    val triples = allPredicates.union(triplesRDD)
//
//    return triples
//  }
//
//
//  def convertIterableToJSONLD(iter :Iterable[Triple]):Vector[String] ={
//    var str=s"""{"@id"${iter.last.getSubject.toString}"""
//
//    iter.foreach(triple => {
//      var tripleString = triple.getSubject.toString + "\t" + triple.getObject.toString + "\t" + triple.getPredicate.toString
//      convertedTriples=convertedTriples:+tripleString
//    })
//
//    return convertedTriples
//  }

  def deleteAndRestart(inputFile:File , outputFormat:String, file: File): Unit ={
    file.delete()
    convertFormat(inputFile, outputFormat)
  }


  def compress(outputCompression:String, output:File): OutputStream = {
    try {
      // file is created here
      val myOutputStream = new FileOutputStream(output.toJava)
      val out: OutputStream = outputCompression match{
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
