package org.dbpedia.databus.client.filehandling.convert.mapping.util

import org.apache.spark.sql.DataFrame

import java.io.PrintWriter

object Tarql_Writer {
  def createTarqlMapFile(tarqlDF: DataFrame, mappingFilePath: String): Unit = {
    val cols = tarqlDF.columns

    println("TARQL DATAFRAME")
    tarqlDF.show(false)

    val prefixesSeq =
      tarqlDF.select(cols(0))
        .distinct()
        .collect()
        .map(row => row.mkString(""))
        .filter(str => !(str.contains("type") && str.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#")))

    val prefixesStr = prefixesSeq.mkString("", "\n", "")

    val tarqlConstruct =
      tarqlDF
        .select(cols(1))
        .distinct()
        .collect()
        .map(row => row.mkString(""))


    var updatedTypeConstructStr = ""

    try {
      updatedTypeConstructStr =
        tarqlConstruct
          .find(str => str.contains("type:type"))
          .map(str => str.split(" ").updated(1, "a").mkString(" "))
          .last
    } catch {
      case none: NoSuchElementException => println("NO TYPE IN TRIPLES INCLUDED")
    }

    val constructStrPart =
      tarqlConstruct
        .map(x => x.split(" ").updated(0, "\t").mkString(" "))
        .filter(str => !str.contains("type:type"))
        .mkString("", "\n", "")

    val constructStr = updatedTypeConstructStr.concat(s"\n$constructStrPart")

    val bindingsSeq = tarqlDF
      .select("bindings")
      .distinct()
      .collect()
      .map(row => row.mkString(""))
      .filter(str => !str.isEmpty)

    val bindingStr = bindingsSeq.mkString("\t", "\n\t", "")

    val tarqlMappingString =
      s"""$prefixesStr
         |
         |CONSTRUCT {
         |$constructStr}
         |WHERE {
         |$bindingStr
         |}
       """.stripMargin

    //    println(s"CALCULATED TARQLSTRING: \n$tarqlMappingString")

    new PrintWriter(mappingFilePath) {
      write(tarqlMappingString)
      close()
    }
  }

  def buildTarqlConstructStr(mappedPredicates: Seq[Seq[String]], predicate: String, bindedSubject: String, bindedPre: String): String = {
    val predicateSeq = mappedPredicates.find(seq => seq.contains(predicate)).get
    s"?$bindedSubject ${predicateSeq(1)}:$predicate ?$predicate$bindedPre;"
  }
}
