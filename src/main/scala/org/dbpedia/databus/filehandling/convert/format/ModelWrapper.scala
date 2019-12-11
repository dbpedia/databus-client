package org.dbpedia.databus.filehandling.convert.format

import org.apache.jena.rdf.model.{Model, ModelFactory}

object ModelWrapper extends Serializable {
  var model: Model = ModelFactory.createDefaultModel()

  def resetModel(): Unit = {
    model = ModelFactory.createDefaultModel()
  }
}
