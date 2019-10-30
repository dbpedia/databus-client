import better.files.File
import org.junit.Test

class Tests {

  @Test
  def fileTest(): Unit = {

    File(File("/home/"), "eisenbahnplatte")
  }
}
