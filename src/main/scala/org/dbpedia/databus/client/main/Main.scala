package org.dbpedia.databus.client.main

import better.files.File
import org.apache.commons.io.FileUtils
import org.dbpedia.databus.client.filehandling.{FileUtil, SourceHandler}
import org.dbpedia.databus.client.main.cli.CLIconf
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {

    println("DBpedia - Databus-Client")

    val conf = new CLIconf(args)
    val cache_dir = File("./target/databus.tmp/cache_dir/")
    if (conf.clear()) FileUtils.deleteDirectory(cache_dir.toJava)
    cache_dir.createDirectoryIfNotExists()

    val target = File(conf.target())
    target.createDirectoryIfNotExists()

    // check output format and compression
    if (!SourceHandler.isSupportedOutFormat(conf.format())) System.exit(1)
    if (!SourceHandler.isSupportedOutCompression(conf.compression())) System.exit(1)


    if (conf.source.isDefined) {

      if (File(conf.source()).exists()) {
        val source: File = File(conf.source())

        if (source.hasExtension && source.extension.get.matches(".sparql|.query")) {
          // file is a query file
          SourceHandler.handleQuery(
            FileUtil.readQueryFile(source),
            target,
            cache_dir,
            conf.format(),
            conf.compression(),
            conf.overwrite())
        }
        else {
          // take already existing files as source
          SourceHandler.handleSource(
            File(conf.source()),
            target,
            conf.format(),
            conf.compression())
        }

      }
      else {
        // conf.source() is a query string
        SourceHandler.handleQuery(
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
