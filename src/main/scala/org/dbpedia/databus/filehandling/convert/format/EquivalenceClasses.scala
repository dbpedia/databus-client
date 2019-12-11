package org.dbpedia.databus.filehandling.convert.format

object EquivalenceClasses extends Enumeration {
  val RDFTypes:Seq[String] = Seq("ttl", "nt", "rdfxml", "jsonld")
  val CSVTypes:Seq[String] = Seq("tsv", "csv")
}
