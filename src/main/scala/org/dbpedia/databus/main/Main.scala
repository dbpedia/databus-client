package org.dbpedia.databus.main

import better.files.File
import org.apache.commons.io.FileUtils
import org.dbpedia.databus.filehandling.FileHandler
import org.dbpedia.databus.filehandling.downloader.Downloader
import org.dbpedia.databus.main.cli.CLIConf
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {

    //    args.foreach(println(_))

    println("Welcome to DBpedia - Databus-Client")

    val conf = new CLIConf(args)
    val cache_dir = File("./target/databus.tmp/cache_dir/")
    val target = File(conf.target())

    if (conf.clear()) FileUtils.deleteDirectory(cache_dir.toJava)
    cache_dir.createDirectoryIfNotExists()

    // check output format and compression
    if (!FileHandler.isSupportedOutFormat(conf.format())) System.exit(1)
    if (!FileHandler.isSupportedOutCompression(conf.compression())) System.exit(1)


    if (conf.source.isDefined) {

      if (File(conf.source()).exists()) {
        val source: File = File(conf.source())

        if (source.extension.get matches(".sparql|.query")) {
          // file is a query file
          FileHandler.handleQuery(
            Downloader.readQueryFile(source),
            target,
            cache_dir,
            conf.format(),
            conf.compression(),
            conf.overwrite())
        }
        else {
          // take already existing files as source
          FileHandler.handleSource(
            File(conf.source()),
            target,
            conf.format(),
            conf.compression())
        }

      }
      else {
        // conf.source() is a query string
        FileHandler.handleQuery(
          conf.source(),
          target,
          cache_dir,
          conf.format(),
          conf.compression(),
          conf.overwrite()
        )
      }

    }
    else {
      LoggerFactory.getLogger("Source Logger").error(s"No source found.")
      println(s"No source set.")
    }
  }


}
