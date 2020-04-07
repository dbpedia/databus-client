package org.dbpedia.databus.filehandling.mapping

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.dbpedia.databus.filehandling.mapping.IDResolution.master

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Create owl:sameAs links for a specific IRI namespace based on the DBpedia id-management
 */
object Links {

  val master: String = "local[*]"
  val drp: Int = Runtime.getRuntime.availableProcessors() * 3

  private val helpMsg: String =
    """
      |mvn scala:run -DmainClass=org.example.MyClass -DaddArgs="<input>|<namespace>|<output>"
      |
      |arguments:
      |  input     - input tsv (DBpedia-Databus: jj-author/id-management/global-ids)
      |  namespcae - namespace to create links for
      |  output    - output directory of created ntriple files
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    if(args.length != 3) { System.err.println(helpMsg); System.exit(1) }
    val sourcePath = args(0)
    val namespace = args(1)
    val targetPath = args(2)

    val spark = SparkSession.builder().master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    implicit val sql: SQLContext = spark.sqlContext
    import sql.implicits._

    val sameThingEntriesDS = sql.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load(sourcePath)
      .drop("singleton_id_base58")
      .as[SameThingEntry]

    val iRINamespace = sql.sparkContext.broadcast(namespace)

    sameAs(sameThingEntriesDS, iRINamespace).repartition(drp)
      .write
      .option("compression", "bzip2")
      .text(targetPath)
  }

  /**
   * DBpedia id-management entry
   * @param original_iri original source IRI
   * @param cluster_id_base58 DBpedia global id
   */
  case class SameThingEntry(original_iri: String, cluster_id_base58: String) {
    lazy val global_iri: String = "https://global.dbpedia.org/id/" + cluster_id_base58
  }

  def sameAs(sameThingEntiesDS: Dataset[SameThingEntry], iRINamespace: Broadcast[String])
            (implicit sql: SQLContext): Dataset[String] = {

    import sql.implicits._

    sameThingEntiesDS.repartition(drp, 'cluster_id_base58)
      .groupByKey(_.cluster_id_base58)
      .flatMapGroups((globalID, sameThingEntries) => {
        val targetNodes: ListBuffer[String] = ListBuffer[String]("https://global.dbpedia.org/id/" + globalID)
        val sourceNodes: ListBuffer[String] = ListBuffer[String]()

        sameThingEntries.foreach(_.original_iri match {
          case sourceNode if sourceNode.startsWith(iRINamespace.value) =>
            sourceNodes.append(sourceNode)
          case targetNode => targetNodes.append(targetNode)
        })

        buildSameAsTriples(sourceNodes.toList, targetNodes.toList)
      })
  }

  private def buildSameAsTriples(sourceNodes: List[String], targetNodes: List[String]): List[String] = {

    sourceNodes.flatMap(sourceNode => {
      targetNodes.map(targetNode => {
        "<"+sourceNode+"> <http://www.w3.org/2002/07/owl#sameAs> <"+targetNode+"> ."
      })
    })
  }
}
