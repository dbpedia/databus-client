package converterTests

import better.files.File
import org.dbpedia.databus.sparql.QueryHandler
import org.scalatest.FlatSpec

class getOutPutFileTest extends FlatSpec{

  "Object" should "give right outFile to inFile" in {
      val inFile = File("/home/eisenbahnplatte/git/databus-client/cache_dir/dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.ttl.bz2")
      val outFormat = "nt"
    val outComp = "gz"
    val src_dir = File("./cache_dir")
    val dest_dir = File("./testTarget/")

//    val outFile = OutPutFileGetter.getOutputFile(inFile, outFormat, outComp, src_dir, dest_dir)

    val comparisonFile = File("./testTarget/marvin/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.nt.gz")
//    assert( outFile == comparisonFile)
  }


//  "Object" should "give right outFile to inFile with new Method" in {
//    val inFile = File("/home/eisenbahnplatte/git/databus-client/cache_dir/dbpedia-mappings.tib.eu/release/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.ttl.bz2")
//    val outFormat = "nt"
//    val outComp = "gz"
//    val dest_dir = File("./testTarget/")
//
//    val outFile = OutPutFileGetter.newGetOutputFile(inFile, outFormat, outComp, dest_dir)
//
//    val comparisonFile = File("./testTarget/marvin/mappings/geo-coordinates-mappingbased/2019.04.20/geo-coordinates-mappingbased_lang=ca.nt.gz")
//    assert( outFile == comparisonFile)
//  }
}

object OutPutFileGetter {
//  def getOutputFile(inputFile: File, outputFormat: String, outputCompression: String, src_dir: File, dest_dir: File): File = {
//
//    val nameWithoutExtension = inputFile.nameWithoutExtension
//    val name = inputFile.name
//    var filepath_new = ""
//    val dataIdFile = inputFile.parent / "dataid.ttl"
//
//    val newOutputFormat = {
//      if (outputFormat == "rdfxml") "rdf"
//      else outputFormat
//    }
//
//    if (dataIdFile.exists) {
//      val dir_structure: List[String] = QueryHandler.executeDataIdQuery(dataIdFile)
//      filepath_new = dest_dir.pathAsString.concat("/")
//      dir_structure.foreach(dir => filepath_new = filepath_new.concat(dir).concat("/"))
//      filepath_new = filepath_new.concat(nameWithoutExtension)
//    }
//    else {
//      // changeExtensionTo() funktioniert nicht bei noch nicht existierendem File, deswegen ausweichen über Stringmanipulation
//      //      filepath_new = inputFile.pathAsString.replace(src_dir.pathAsString, dest_dir.pathAsString.concat("/NoDataID"))
//      filepath_new = dest_dir.pathAsString.concat("/NoDataID").concat(inputFile.pathAsString.replace(File(".").pathAsString, "")) //.concat(nameWithoutExtension)
//
//      filepath_new = filepath_new.replaceAll(name, nameWithoutExtension)
//    }
//
//    if (outputCompression.isEmpty) {
//      filepath_new = filepath_new.concat(".").concat(newOutputFormat)
//    }
//    else {
//      filepath_new = filepath_new.concat(".").concat(newOutputFormat).concat(".").concat(outputCompression)
//    }
//
//    val outputFile = File(filepath_new)
//    //create necessary parent directories to write the outputfile there, later
//    outputFile.parent.createDirectoryIfNotExists(createParents = true)
//
//    println(s"converted file:\t${outputFile.pathAsString}\n")
//
//    outputFile
//  }

//  def newGetOutputFile(inputFile: File, outputFormat: String, outputCompression: String, dest_dir: File): File = {
//
//    val nameWithoutExtension = inputFile.nameWithoutExtension
//
//    val dataIdFile = inputFile.parent / "dataid.ttl"
//
//    val newOutputFormat = {
//      if (outputFormat == "rdfxml") "rdf"
//      else outputFormat
//    }
//
//    val outputDir = {
//      if (dataIdFile.exists) QueryHandler.getTargetDir(dataIdFile, dest_dir)
//      else
//        File(dest_dir.pathAsString.concat("/NoDataID")
//          .concat(inputFile.pathAsString.splitAt(inputFile.pathAsString.lastIndexOf("/"))._1
//            .replace(File(".").pathAsString, "")
//          )
//        )
//    }
//
//    val newName = {
//      if (outputCompression.isEmpty)  s"$nameWithoutExtension.$newOutputFormat"
//      else  s"$nameWithoutExtension.$newOutputFormat.$outputCompression"
//    }
//
//    val outputFile = outputDir / newName
//
//    //create necessary parent directories to write the outputfile there, later
//    outputFile.parent.createDirectoryIfNotExists(createParents = true)
//
//    println(s"converted file:\t${outputFile.pathAsString}\n")
//
//    outputFile
//  }
}
