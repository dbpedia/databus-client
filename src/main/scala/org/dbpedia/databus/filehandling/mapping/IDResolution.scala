package org.dbpedia.databus.filehandling.mapping

import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

/**
 * Replace {s,p,o} IRIs for RDFTriple dataset
 */
object IDResolution {

  val master: String = "local[*]"
  val drp: Int = Runtime.getRuntime.availableProcessors() * 3

  case class NTStatement(sConstruct: String, pConstruct: String, oConstruct: String) {
    override def toString: String = "<"+sConstruct+"> <"+pConstruct+"> <"+oConstruct+" ."
  }

  case class SameThingEntry(localIRI: String, globalID: String) {
    lazy val globalIRI: String = "https://global.dbpedia.org/id/" + globalID
  }

  def main(args: Array[String]): Unit = {

    // databus:generic/interlanguage-links/2020.02.01/interlanguage-links_lang=en.ttl.bz2
    val ntPath = "data/interlanguage-links_lang=en.ttl.bz2"
    // databus:jj-author/id-management/global-ids/2020.01.30/global-ids_base58.tsv.bz2
    val stPath = "data/global-ids_base58.tsv.bz2"

    val spark = SparkSession.builder().master(master).getOrCreate()
    implicit val sql: SQLContext = spark.sqlContext
    import sql.implicits._

    val nTriplesDS = sql.read.textFile().flatMap(inputRecord => {
      inputRecord.split(" ", 4) match {
        case Array(s, p, o, ".") if o.startsWith("<")=>
          Some(NTStatement(
            s.substring(1, s.length - 1),
            p.substring(1, p.length - 1),
            o.substring(1, o.length - 1)
          ))
        case _ => None
      }
    }).repartition(drp)

    mapNodeIDs(nTriplesDS, sql.createDataset(List[SameThingEntry]()))
  }

  /**
   * TODO
   *  - overload: non spark, broadcast mapping, etc.
   *  - implement mappingDS[sourceIRI, targetIRI] instead of sameThingDS
   */
  def mapNodeIDs(nTriplesDS: Dataset[NTStatement], sameThingEntriesDS: Dataset[SameThingEntry])
                (implicit sql: SQLContext): Dataset[NTStatement] = {

    import sql.implicits._

    nTriplesDS.repartition(drp, $"sConstruct")
    sameThingEntriesDS.repartition(drp, $"localIRI")

    val subjectsReplaced = nTriplesDS.joinWith(
      sameThingEntriesDS,
      nTriplesDS("sConstruct") === sameThingEntriesDS("localIRI"),
      joinType = "left_outer").map {
      case (originalNTStatement, mapping) => originalNTStatement.copy(sConstruct = mapping.globalIRI)
      case (originalNTStatement, _) => originalNTStatement
    } repartition(drp, $"oConstruct")

    subjectsReplaced.joinWith(
      sameThingEntriesDS,
      subjectsReplaced("oConstruct") === sameThingEntriesDS("localIRI"),
      joinType = "left_outer").map {
      case (originalNTStatement, mapping) => originalNTStatement.copy(oConstruct = mapping.globalIRI)
      case (originalNTStatement, _) => originalNTStatement
    }
  }

  def writeNTStatements(nTriplesDS: Dataset[NTStatement])
                       (implicit sql: SQLContext): Unit = {

    import sql.implicits._

    nTriplesDS.map(_.toString).show(20, 0)
  }
}
