package cliTests

import org.dbpedia.databus.main.cli.CLIConf
import org.scalatest.FlatSpec

class cliTest extends FlatSpec {

  "DatabusClient" should "handle not set params of Cli right" in {
    val conf = new CLIConf(Array[String]())

    if (!conf.query.isDefined) println("query is not set")
  }
}
