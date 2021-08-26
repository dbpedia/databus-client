package org.dbpedia.databus.client.filehandling

import better.files.File

class CompileConfig(val inputFormat: String,
                   val inputCompression: String,
                   val outputFormat: String,
                   val outputCompression: String,
                   val target: File,
                   val mapping: String,
                   val delimiter: Character,
                   val quotation: Character,
                   val createMapping: Boolean,
                    val graphURI: String,
                   val outFile: File) {

  var sha = ""
}
