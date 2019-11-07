package mapping

import better.files.File
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbpedia.databus.filehandling.FileUtil
import org.dbpedia.databus.filehandling.converter.mappings.TSV_Writer
import org.dbpedia.databus.filehandling.converter.rdf_reader.TTL_Reader
import org.scalatest.FlatSpec
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.immutable.Vector

class tsv_writer_tests extends FlatSpec {

  "rdd[Triple]" should "be saved to tsv and belonging Sparql Query to read it back to RDD[Triple] should be written as well" in {
    //"/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/writeTSV/testBob.ttl"
    val inputFile = File("/home/eisenbahnplatte/git/databus-client/src/resources/test/MappingTests/writeTSV/testBob.ttl")
    val tempDir = inputFile.parent / "temp"
    if (tempDir.exists) tempDir.delete()
    val headerTempDir = inputFile.parent / "tempheader"
    val targetFile: File = inputFile.parent / inputFile.nameWithoutExtension.concat(".tsv")

    val spark = SparkSession.builder()
      .appName(s"Triple reader")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext

    val triplesRDD = TTL_Reader.readTTL(spark, inputFile)

    val solution = TTLWriter.convertToTSV(triplesRDD, spark)
    solution(1).write.option("delimiter", "\t").option("nullValue", "?").option("treatEmptyValuesAsNulls", "true").csv(tempDir.pathAsString)
    solution(0).write.option("delimiter", "\t").csv(headerTempDir.pathAsString)

    FileUtil.unionFilesWithHeaderFile(headerTempDir, tempDir, targetFile)
  }

}

object TTLWriter {

  def getSplitPredicate(pre:String): (String, String)={
    val lastHashkeyIndex = pre.lastIndexOf("#")
    val lastSlashIndex = pre.lastIndexOf("/")
    var split = ("","")

    if (lastHashkeyIndex >= lastSlashIndex) split =pre.splitAt(pre.lastIndexOf("#")+1)
    else split =pre.splitAt(pre.lastIndexOf("/")+1)

    split
  }

    def convertToTSV(data: RDD[Triple], spark: SparkSession): Vector[DataFrame] = {
      val sql = spark.sqlContext

      import sql.implicits._
      //Gruppiere nach Subjekt, dann kommen TripleIteratoren raus
      val triplesGroupedBySubject = data.groupBy(triple ⇒ triple.getSubject).map(_._2)
      val allPredicates = data.groupBy(triple => triple.getPredicate.getURI).map(_._1) //WARUM GING ES NIE OHNE COLLECT MIT FOREACHPARTITION?

      var headerVector = Vector[String]("resource")
      var mappingPredicates = Vector.empty[Vector[String]]

      allPredicates.collect.foreach(pre => {
        val split = getSplitPredicate(pre:String)
        mappingPredicates = mappingPredicates :+ Vector((split._2),(split._1))

        headerVector = headerVector :+ split._2
      })

      println("MAPPINGVECTOR")

      mappingPredicates.foreach(x => x.foreach(println(_)))

      var sparqlString = ""
      mappingPredicates.foreach(pre => {
        sparqlString= sparqlString.concat(s"PREFIX xxx${pre(0)}: ${pre(1)}\n")
      })

      println(sparqlString)

      val triplesTSV = triplesGroupedBySubject.map(allTriplesOfSubject => convertAllTriplesOfSubjectToTSV(allTriplesOfSubject, headerVector, mappingPredicates)._1)

      val triplesDS = sql.createDataset(triplesTSV)
      val triplesDF = triplesDS.select((0 until headerVector.size).map(r => triplesDS.col("value").getItem(r)): _*)
      val headerDS = sql.createDataset(Vector((headerVector)))
      val headerDF = headerDS.select((0 until headerVector.size).map(r => headerDS.col("value").getItem(r)): _*)
      //    headerDF.show(false)
      triplesDF.show(false)


      Vector(headerDF, triplesDF)
    }

    def convertAllTriplesOfSubjectToTSV(triples: Iterable[Triple], allPredicates: Vector[String], mappingPredicates: Vector[Vector[String]]): (Seq[String],Seq[String]) = {
      var TSVseq: IndexedSeq[String] = IndexedSeq.fill(allPredicates.length){new String}
      TSVseq = TSVseq.updated(0,triples.last.getSubject.getURI)

      println(TSVseq)
      var tarqlBindPart = Seq[String](s"BIND(URI(?${allPredicates(0)}) AS ?${allPredicates(0)}Binded)")
      var tarqlConstruct = Seq[String](s"?${allPredicates(0)}Binded a ?${allPredicates(0)}Binded)")

        triples.foreach(triple => {
          var alreadyIncluded = false
          var tripleObject = ""

          val triplePredicate = getSplitPredicate(triple.getPredicate.getURI)._2
          if (allPredicates.contains(triplePredicate)) {

            alreadyIncluded = true

              if (triple.getObject.isLiteral) {
                val datatype = getSplitPredicate(triple.getObject.getLiteralDatatype.getURI)._2
                tarqlBindPart = tarqlBindPart :+ s"BIND(xsd:$datatype(?$triplePredicate) AS ?${triplePredicate}Binded)"
                tripleObject = triple.getObject.getLiteralLexicalForm
              }
              else if (triple.getObject.isURI) {
                tripleObject = triple.getObject.getURI
                tarqlBindPart = tarqlBindPart :+ s"BIND(URI(?$triplePredicate) AS ?${triplePredicate}Binded)"
              }
              else tripleObject = triple.getObject.getBlankNodeLabel

          }

          val index = allPredicates.indexOf(triplePredicate)

          if (alreadyIncluded == true) {
            TSVseq = TSVseq.updated(index, tripleObject)
          }
          else {
            TSVseq = TSVseq.updated(index, "")
          }
        })

      println("TARQLSTRING")
      println(tarqlBindPart)

      println("TSVSEQ")
      println(TSVseq)
      (TSVseq, tarqlBindPart)
    }


}