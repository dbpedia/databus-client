package org.dbpedia.databus.client.filehandling.convert

import better.files.File
import org.dbpedia.databus.client.Config
import org.dbpedia.databus.client.filehandling.{FileUtil, Utils}
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler

import java.io.FileWriter

class ConvertConfig(inputFile:File, cliConfig: CLI_Config) {

  val inFile: File = inputFile
  val target:File = File(cliConfig.target())
  var endpoint:String = _
  var outFormat: String = cliConfig.format()
  var outCompression: String = cliConfig.compression()

  val mapping: String = cliConfig.mapping()
  val delimiter:Char = cliConfig.delimiter().toCharArray.head
  val quotation:Char = cliConfig.quotation().toCharArray.head
  val createMapping: Boolean = cliConfig.createMapping()
  val graphURI:String = cliConfig.graphURI()
  val baseURI:String = cliConfig.baseURI()
  var inFormat:String = _
  var inCompression:String = _
  var outFile:File = _
  var sha = ""

  def init(): ConvertConfig ={
    inCompression = FileUtil.getCompressionType(inputFile)
    inFormat = FileUtil.getFormatType(inputFile, inCompression)

    sha = {
      if (FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt")) != "") FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt"))
      else FileUtil.getSha256(inputFile)
    }

    if (outFormat=="same") outFormat = inFormat
    if (outCompression=="same") outCompression = inCompression
    outFile = getOutputFile(inputFile, inCompression)

    endpoint = {
      if(cliConfig.endpoint.isDefined) cliConfig.endpoint()
      else Config.endpoint
    }

    this
  }



  /**
   * calculate output file depending on input file and desired output compression and format
   *
   * @param inputFile input file
   * @return output file
   */
  private def getOutputFile(inputFile: File, inCompression: String): File = {

    val nameWithoutExtension = inputFile.nameWithoutExtension

    val dataIdFile = inputFile.parent / "dataid.jsonld"

    val newOutputFormat = {
      if (outFormat == "rdfxml") "rdf"
      else outFormat
    }

    val outputDir = {
      if (dataIdFile.exists) {
        val pgav = QueryHandler.getTargetDir(dataIdFile)
        val fw = new FileWriter((target / "identifiers_downloadedFiles.txt").pathAsString, true)
        try {
          fw.append(s"${Utils.urlOneUp(Config.endpoint)}$pgav/${inputFile.name}\n")
        }
        finally fw.close()

        File(s"${target.pathAsString}/$pgav")
      }
      else
        File(target.pathAsString.concat("/NoDataID")
          .concat(inputFile.pathAsString.splitAt(inputFile.pathAsString.lastIndexOf("/"))._1
            .replace(File(".").pathAsString, "")
          )
        )
    }

    val newName = {
      if (outCompression.isEmpty) s"$nameWithoutExtension.$newOutputFormat"
      else s"$nameWithoutExtension.$newOutputFormat.$outCompression"
    }

    val outputFile = outputDir / newName

    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    println(s"output file:${outputFile.pathAsString}\n")

    outputFile
  }
}
