package org.dbpedia.databus.client.filehandling

import better.files.File
import org.dbpedia.databus.client.main.CLI_Config
import org.dbpedia.databus.client.sparql.QueryHandler
import org.slf4j.LoggerFactory

import java.io.FileWriter

class CompileConfig(inputFile:File, cliConfig: CLI_Config) {

  val inFile: File = inputFile
  var outFormat: String = cliConfig.format()
  var outCompression: String = cliConfig.compression()
  val target:File = File(cliConfig.target())
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

  def init(): CompileConfig ={
    inCompression = FileUtil.getCompressionType(inputFile)
    inFormat = FileUtil.getFormatType(inputFile, inCompression)

    sha = {
      if (FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt")) != "") FileUtil.getShaOfFileInCache(inputFile, File("./target/databus.tmp/cache_dir/shas.txt"))
      else FileUtil.getSha256(inputFile)
    }

    if (outFormat=="same") outFormat = inFormat
    if (outCompression=="same") outCompression = inCompression
    outFile = getOutputFile(inputFile, inCompression)

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

    val dataIdFile = inputFile.parent / "dataid.ttl"

    val target_dir = File(cliConfig.target())

    val newOutputFormat = {
      if (cliConfig.format() == "rdfxml") "rdf"
      else cliConfig.format()
    }

    val outputDir = {
      if (dataIdFile.exists) {
        val pgav = QueryHandler.getTargetDir(dataIdFile)
        val fw = new FileWriter((target_dir / "identifiers_downloadedFiles.txt").pathAsString, true)
        try {
          fw.append(s"https://databus.dbpedia.org/$pgav/${inputFile.name}\n")
        }
        finally fw.close()

        File(s"${target_dir.pathAsString}/$pgav")
      }
      else
        File(target_dir.pathAsString.concat("/NoDataID")
          .concat(inputFile.pathAsString.splitAt(inputFile.pathAsString.lastIndexOf("/"))._1
            .replace(File(".").pathAsString, "")
          )
        )
    }

    val newName = {
      if (cliConfig.compression().isEmpty || cliConfig.compression()=="same" && inCompression=="") s"$nameWithoutExtension.$newOutputFormat"
      else s"$nameWithoutExtension.$newOutputFormat.${cliConfig.compression()}"
    }

    val outputFile = outputDir / newName

    //create necessary parent directories to write the outputfile there, later
    outputFile.parent.createDirectoryIfNotExists(createParents = true)

    println(s"output file:\t${outputFile.pathAsString}\n")

    outputFile
  }
}
