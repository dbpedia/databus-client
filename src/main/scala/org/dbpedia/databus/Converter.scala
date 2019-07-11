package org.dbpedia.databus

import java.io.{BufferedInputStream, BufferedWriter, FileOutputStream, FileWriter, InputStream, OutputStream}

import scala.sys.process._
import scala.language.postfixOps
import scala.util.control.Breaks._
import scala.io.Source
import better.files.File
import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.io.FileUtils
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import net.sansa_stack.rdf.spark.partition.RDFPartition
import org.apache.spark.HashPartitioner
import org.apache.spark._
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.jena.graph.{NodeFactory, Triple}

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

    val outputFormat = "nt"

    val conf = new SparkConf().setAppName(s"Triple reader  ${inputFile.name}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName(s"Triple reader  ${inputFile.name}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println(inputFile.pathAsString)
    var data = NTripleReader.load(spark, inputFile.pathAsString)
    val tempDir = s"${inputFile.parent.pathAsString}/temp"
    val targetFile:File = inputFile.parent / inputFile.nameWithoutExtension.concat(s".$outputFormat")

    println(data.getClass)

    data.take(5)

    val tempDir2="test/temp"
    val targetFile2=File("test/result")

//    val sortedTriples = data.map(triple ⇒ (triple.getSubject, triple.toString))
//      .groupByKey();
//
//    val sortedTriples = sc.parallelize(Seq(new Triple(NodeFactory.createBlankNode(),NodeFactory.createBlankNode(),NodeFactory.createBlankNode())))
//    println(sortedTriples.getClass)
    var tripleSeq: Seq[String] = Seq()
    var list = List[String]()//List(new Triple(NodeFactory.createBlankNode(),NodeFactory.createBlankNode(),NodeFactory.createBlankNode()))
    list = "hallo"::list

    val sortedIterTriples = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
//    sortedIterTriples.foreach(iterTriple => iterTriple.foreach(triple => println(triple)))
    sortedIterTriples.foreach(iterTriple => iterTriple.foreach(triple => {list = triple.toString::list}))
//    sortedTriples.productIterator.foreach(println)

    list.foreach(x=>println(x))

    //listb.toList.foreach(x => println(x))

    val sortedTripleRDD = sc.parallelize(tripleSeq)

//    data.groupBy(triple ⇒ triple.getSubject).foreach(_._2.foreach(x => println(x.getSubject.toString())))


    sortedTripleRDD.saveAsTextFile(tempDir2)

//    var subjects = data.g
//    subjects.getPartition(1)

//    val y = data.groupBy(word => word.charAt(0))
    // y: org.apache.spark.rdd.RDD[(Char, Iterable[String])] =
    //  ShuffledRDD[18] at groupBy at <console>:23

//    y.collect

//    try {
//
//      val findTripleFiles = s"find $tempDir2/ -name part*" !!
//      val concatFiles = s"cat $findTripleFiles" #> targetFile2.toJava !
//
//      if( concatFiles == 0 ) FileUtils.deleteDirectory(File(tempDir2).toJava)
//      else System.err.println(s"[WARN] failed to merge $tempDir2/*")
//    }
//    catch {
//      case fileAlreadyExists: RuntimeException => deleteAndRestart(inputFile: File, outputFormat: String, targetFile: File)
//    }
    return targetFile
  }

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
