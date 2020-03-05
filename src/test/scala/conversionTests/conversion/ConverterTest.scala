package conversionTests.conversion

import java.io._

import better.files.File
import org.apache.jena.atlas.iterator.IteratorResourceClosing
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.api.Databus
import org.dbpedia.databus.api.Databus.{Compression, Format}
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.filehandling.FileUtil.copyStream
import org.dbpedia.databus.filehandling.convert.compression.Compressor
import org.dbpedia.databus.filehandling.convert.format.rdf.RDFHandler
import org.dbpedia.databus.filehandling.convert.format.rdf.read.{RDF_Reader, TTL_Reader}
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class ConverterTest extends FlatSpec {
  val spark: SparkSession = SparkSession.builder()
    .appName(s"Triple reader")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val sparkContext: SparkContext = spark.sparkContext
  sparkContext.setLogLevel("WARN")

  "RDD" should "not contain empty Lines" in {

    val file = File("/home/eisenbahnplatte/git/databus-client/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-conversion-testbed_bob4.ttl")

    readTriples(file).foreach(println(_))

    readTriplesWithRDFReader(file).foreach(println(_))

  }

  def readTriples(file: File): RDD[Triple] = {
    val spark = SparkSession.builder()
      .appName(s"Triple reader")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    TTL_Reader.read(spark, file)
  }

  def readTriplesWithRDFReader(file: File): RDD[Triple] = {


    RDF_Reader.read(spark, file)
  }


  "Written File" should "not contain empty Lines" in {

    val file = File("/home/eisenbahnplatte/git/databus-client/src/resources/databus-client-testbed/format-testbed/2019.08.30/format-conversion-testbed_bob4.ttl")

    val triples = readTriples(file)

    triples.foreach(println(_))
    val outFile = File("./test/testFile.nt")
    writeNtripleFileWithSansa(triples, outFile)
    println(s"Count Lines $outFile: ${outFile.lineCount}")


    //withoutSansa
    val outFile2 = File("./test/testFile2.nt")
    writeNtripleFile(triples, outFile2)
    println(s"Count Lines $outFile2: ${outFile2.lineCount}")
    println("triples contain Prefixes. Not good.")

    val outFile3 = File("./test/testFile3.nt")
    writeTriples(triples, outFile3)
    println(s"Count Lines $outFile3: ${outFile3.lineCount}")
  }

  def writeTriples(triples: RDD[Triple], outFile: File): Unit = {
    val tempDir = outFile.parent / "temp"

    triples.map(triple => {
      val os = new ByteArrayOutputStream()
      RDFDataMgr.writeTriples(os, Iterator[Triple](triple).asJava)
      os.toString.trim
    }).saveAsTextFile(tempDir.pathAsString)

    FileUtil.unionFiles(tempDir, outFile)
  }


  def writeNtripleFileWithSansa(triples: RDD[Triple], outFile: File): Unit = {
    //
    //    triples.saveAsNTriplesFile(tempDir.pathAsString)
    //    FileUtil.unionFiles(tempDir, outFile)
  }


  def writeNtripleFile(triples: RDD[Triple], outFile: File): Unit = {
    val tempDir = outFile.parent / "temp"

    triples.saveAsTextFile(tempDir.pathAsString)
    FileUtil.unionFiles(tempDir, outFile)
  }


  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns")
    t1 - t0
  }

  "Conversion" should "not be too slow" in {

    time(RDFHandler.readRDF(File("/home/eisenbahnplatte/git/databus-client/src/resources/test/SpeedTest/specific-mappingbased-properties_lang=ca.ttl.bz2"), "ttl", spark))

    val triples =RDFHandler.readRDF(File("/home/eisenbahnplatte/git/databus-client/src/resources/test/SpeedTest/specific-mappingbased-properties_lang=ca.ttl.bz2"), "ttl", spark)

//    time(Converter.writeTriples(File("/home/eisenbahnplatte/git/databus-client/src/resources/test/SpeedTest/specific-mappingbased-properties_lang=ca.ttl.bz2"), triples, "nt", spark))

    time(Databus.source("./src/resources/test/SpeedTest/specific-mappingbased-properties_lang=ca.ttl.bz2").compression(Compression.bz2).format(Format.nt).execute())
//    Databus.source("./src/resources/test/SpeedTest/specific-mappingbased-properties_lang=ca.ttl.bz2").compression(Compression.bz2).format(Format.nt).execute()


  }

  "Copy" should "not be too slow" in {
    val compressedOutStream = Compressor.compress("gz", File("src/resources/test/SpeedTest/test"))
    //file is written here
    println("test")
    copyStream(new FileInputStream(File("./src/resources/test/SpeedTest/temp/specific-mappingbased-properties_lang=ca.nt").toJava), compressedOutStream)


    val compressedOutStream2 = Compressor.compress("", File("src/resources/test/SpeedTest/test2"))
    copyStream(new FileInputStream(File("./src/resources/test/SpeedTest/temp/specific-mappingbased-properties_lang=ca.nt").toJava), compressedOutStream2)
  }

  "Spark Ntriple Read" should "be as fast as Sansa NTripleReader" in {

    //    def sansaRead(filePath: String): RDD[Triple] = {
    //      val spark = SparkSession.builder()
    //        .appName(s"Triple reader")
    //        .master("local[*]")
    //        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //        .getOrCreate()
    //      NTripleReader.load(spark, filePath, ErrorParseMode.SKIP, WarningParseMode.IGNORE, checkRDFTerms = false, LoggerFactory.getLogger("ErrorlogReadTriples"))
    //    }
    //
    //    def sparkRead(filePath:String): RDD[Triple]={
    //
    //      val spark = SparkSession.builder()
    //        .appName(s"Triple reader")
    //        .master("local[*]")
    //        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //        .getOrCreate()
    //
    //        val sc = spark.sparkContext
    //        val rdd = sc.textFile(filePath, 20)
    //        var triplesRDD = sc.emptyRDD[Triple]
    //
    //
    //        rdd.mapPartitions(
    //          part => {
    //            val input = ReadableByteChannelFromIterator.toInputStream(part.asJava)
    //            val it = RiotParsers.createIteratorNTriples(input, null)
    //            new IteratorResourceClosing[Triple](it, input).asScala
    //          }
    //        )
    //      }

    def readNTriplesWithoutSansa(filePath: String): RDD[Triple] = {
      val spark = SparkSession.builder()
        .appName(s"Triple reader")
        .master("local[*]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

      val sc = spark.sparkContext
      val rdd = sc.textFile(filePath, 20)


      rdd.mapPartitions(
        part => {
          val input: InputStream = new SequenceInputStream({
            val i = part.map(s => new ByteArrayInputStream(s.getBytes("UTF-8")))
            i.asJavaEnumeration
          })

          val it = RiotParsers.createIteratorNTriples(input, null)
          new IteratorResourceClosing[Triple](it, input).asScala
        }
      )
    }

    val filePath2 = "./src/resources/test/converterTests.ConverterTest/file2.nt"

    //    println("sansaread File1")
    //    val timeSansa= time {sansaRead(filePath1)}
    //
    //    println("sparkRead File1")
    //    val timeRiot = time(sparkRead(filePath1))
    //
    //
    //    assert(timeRiot<=timeSansa)
    //
    //    println("sansaread File2")
    //    val timeSansa2 = time {sansaRead(filePath2)}
    //
    //    println("sparkRead File2")
    //    val timeRiotParser2 = time(sparkRead(filePath2))

    println("readNTRiplesWithoutSansa")
    time(readNTriplesWithoutSansa(filePath2))
    //
    //    assert(timeRiotParser2 <= timeSansa2)
  }

  "Converter" should "success" in {
    //    Converter.readTriples()
  }
}

private object NonSerializableObjectWrapper {
  def apply[T: ClassTag](constructor: => T): NonSerializableObjectWrapper[T] = new NonSerializableObjectWrapper[T](constructor)
}

private class NonSerializableObjectWrapper[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
  @transient private lazy val instance: T = constructor

  def get: T = instance
}

