package org.dbpedia.databus

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class InputstreamReceiver(input: InputStream) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {


    def onStart() {
      // Start the thread that receives data over a connection
      println("hallo")
      new Thread("InputStream Receiver") {
        override def run() { receive() }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself if isStopped() returns false
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private def receive() {
      var userInput: String = null
      try {

        // Until stopped or connection broken continue reading
        val reader = new BufferedReader(new InputStreamReader(input))
        userInput = reader.readLine()
        println(s"userInput: $userInput")
        while(!isStopped && userInput != null) {
          store(userInput)
          userInput = reader.readLine()
        }
        reader.close()
      } catch {
        case t: Throwable =>
          // restart if there is any other error
          restart("Error receiving data", t)
      }
    }


}
