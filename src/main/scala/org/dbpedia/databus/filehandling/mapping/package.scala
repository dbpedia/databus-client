package org.dbpedia.databus.filehandling

package object mapping {

  /**
   * DBpedia id-management entry
   * @param original_iri original source IRI
   * @param cluster_id_base58 DBpedia global id
   */
  case class SameThingEntry(original_iri: String, cluster_id_base58: String) {
    lazy val global_iri: String = "https://global.dbpedia.org/id/" + cluster_id_base58
  }

  case class SameAs(sourceIRI: String, targetIRI: String)
}
