package org.dbpedia.databus.client.filehandling.convert

object EquivalenceClasses extends Enumeration {

  val RDF_TRIPLES: Seq[String] = Seq(
    "ttl",
    "nt",
    "rdfxml",
    "jsonld"
  )

  val RDF_QUADS: Seq[String] = Seq(
    "nq",
    "trix",
    "trig"
  )

  val TSD: Seq[String] = Seq(
    "tsv",
    "csv"
  )

}
