package org.dbpedia.databus.filehandling.mapping

import java.io.File

import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * Replaces subject IRIs for RDFTriple datasets with sameAs IRIs of selected namespace
 */
object IDResolution {

  val master: String = "local[*]"
  val drp: Int = Runtime.getRuntime.availableProcessors() * 3

  private val helpMsg: String =
    """
      |mvn scala:run -DmainClass=org.dbpedia.databus.filehandling.mapping.IDResolution -DaddArgs="<input>|<mapping>|<namespace>|<output>"
      |
      |arguments:
      |  input     - input NTriples file or dir of files (DBpedia-Databus: dbpedia/labels|comments|abstracts)
      |  mapping   - input tsv (DBpedia-Databus: jj-author/id-management/global-ids)
      |  namespace - namespace to create links for
      |  output    - output directory of created NTriples files
      |""".stripMargin

  case class RawStatement(sIRI: String, pIRI: String, rest: String) {
    override def toString: String = {
      "<" + sIRI + "> <" + pIRI + "> " + rest
    }
  }


  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println(helpMsg);
      System.exit(1)
    }
    val inputPath = args(0)
    val mappingsPath = args(1)
    val namespace = args(2)
    val targetPath = args(3)
    new File(targetPath).mkdirs()

    val spark = SparkSession.builder().master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    implicit val SQLContext: SQLContext = spark.sqlContext

    val mappingDS = loadSameAs(mappingsPath, namespace)

    new File(inputPath) match {
      case file if file.isFile =>
        resolve(file.getAbsolutePath, mappingDS)
          .write
          .option("compression", "bzip2")
          .text(targetPath + "/" + file.getName)
      case dir if dir.isFile =>
        dir.listFiles().filter(_.isFile).foreach(file => {
          resolve(file.getAbsolutePath, mappingDS)
            .write
            .option("compression", "bzip2")
            .text(targetPath + "/" + file.getName)
        })
      case _ => System.err.println("neither file or dir as input")
    }
  }

  def resolve(inputPath: String, mappingDS: Dataset[SameAs])
             (implicit SQLContext: SQLContext): Dataset[String] = {

    import SQLContext.implicits._

    val sourceDS = SQLContext.read.textFile(inputPath)
      .repartition(Runtime.getRuntime.availableProcessors() * 3)
      .flatMap(toRawStatement)
      .repartition(drp, 'sIRI)

    sourceDS
      .joinWith(
        mappingDS,
        sourceDS("sIRI") === mappingDS("sourceIRI"),
        joinType = "inner"
      ).map({ case (originalRawStatement, mapping) => originalRawStatement.copy(sIRI = mapping.targetIRI) })
      .repartition(drp)
      .map(_.toString)
  }

  def loadSameAs(mappingsPath: String, namespace: String)(implicit SQLContext: SQLContext): Dataset[SameAs] = {

    import SQLContext.implicits._
    SQLContext.read
      .format("csv")
      .option("header", "true").option("delimiter", "\t")
      .load(mappingsPath)
      .drop("singleton_id_base58")
      .as[SameThingEntry]
      .groupByKey(_.cluster_id_base58)
      .flatMapGroups((_, sameThingEntries) => {
        val sourceIRIs: ListBuffer[String] = ListBuffer[String]()
        val targetIRIs: ListBuffer[String] = ListBuffer[String]()

        sameThingEntries.foreach(_.original_iri match {
          case targetIRI if targetIRI.startsWith(namespace) =>
            targetIRIs.append(targetIRI)
          case sourceIRI => sourceIRIs.append(sourceIRI)
        })
        targetIRIs.flatMap(targetIRI => {
          sourceIRIs.map(sourceIRI => {
            SameAs(sourceIRI, targetIRI)
          })
        })
      })
      .repartition(drp, 'sourceIRI)
  }

  def toRawStatement(string: String): Option[RawStatement] = {
    val Array(s, p, rest): Array[String] = string.split(" ", 3)
    try {
      if (s.charAt(0) == '<' && p.charAt(0) == '<')
        Some(RawStatement(s.drop(1).dropRight(1), p.drop(1).dropRight(1), rest))
      else None
    } catch {
      case _: ArrayIndexOutOfBoundsException => None
    }
  }

  //  /**
  //   * TODO
  //   *  - overload: non spark, broadcast mapping, etc.
  //   *  - implement mappingDS[sourceIRI, targetIRI] instead of sameThingDS
  //   */
  //  def mapNodeIDs(nTriplesDS: Dataset[NTStatement], sameThingEntriesDS: Dataset[SameThingEntry])
  //                (implicit sql: SQLContext): Dataset[NTStatement] = {
  //
  //    import sql.implicits._
  //
  //    nTriplesDS.repartition(drp, $"sConstruct")
  //    sameThingEntriesDS.repartition(drp, $"localIRI")
  //
  //    val subjectsReplaced = nTriplesDS.joinWith(
  //      sameThingEntriesDS,
  //      nTriplesDS("sConstruct") === sameThingEntriesDS("localIRI"),
  //      joinType = "left_outer").map {
  //      case (originalNTStatement, mapping) => originalNTStatement.copy(sConstruct = mapping.globalIRI)
  //      case (originalNTStatement, _) => originalNTStatement
  //    } repartition(drp, $"oConstruct")
  //
  //    subjectsReplaced.joinWith(
  //      sameThingEntriesDS,
  //      subjectsReplaced("oConstruct") === sameThingEntriesDS("localIRI"),
  //      joinType = "left_outer").map {
  //      case (originalNTStatement, mapping) => originalNTStatement.copy(oConstruct = mapping.globalIRI)
  //      case (originalNTStatement, _) => originalNTStatement
  //    }
  //  }
}
