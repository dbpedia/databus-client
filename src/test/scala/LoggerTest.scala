import java.nio.file.NoSuchFileException

import better.files.File
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class LoggerTest extends FlatSpec {

  info("starting...")

  "logger" should "print a File" in {
    val logger = LoggerFactory.getLogger("Logger")


    logger.debug("whahwhaw")
    logger.info("Temperature has risen above 50 degrees.")

  }

  "logger" should "print something" in {
    try {
      File("hallo").delete()
    }
    catch {
      case noSuchFileException: NoSuchFileException => LoggerFactory.getLogger("test").error("File does not exist") //deleteAndRestart(inputFile, inputFormat, outputFormat, targetFile: File)
    }
  }
}
