package org.dbpedia.databus.filehandling.downloader

import java.io.{IOException, InputStream}

class LoggingInputStream(in: InputStream, length: Long, step: Long) extends InputStream {

  private val nanos: Long = System.nanoTime()
  private var next = step
  private val pretty = true
  private var bytes = 0L

  override def read(): Int = {
    val read = in.read
    if (read != -1) count(1)
    read
  }

  override def read(buf: Array[Byte]): Int = {
    val read = in.read(buf)
    if (read != -1) count(read)
    read
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    val read = in.read(buf, off, len)
    if (read != -1) count(read)
    read
  }

  override def skip(skip: Long): Long = {
    val read = in.skip(skip)
    count(read)
    read
  }

  override def available: Int = {
    in.available
  }

  override def close(): Unit = {
    count(0L, close = true)
    in.close()
  }

  private def formatBytes(bytes: Long): String = {
    if (bytes < 0) "? B"
    else if (bytes < 1024) bytes + " B"
    else if (bytes < 1048576) (bytes / 1024F) + " KB"
    else if (bytes < 1073741824) (bytes / 1048576F) + " MB"
    else (bytes / 1073741824F) + " GB"
  }

  private def zeros(num: Long): String = if (num < 10) "0" + num else num.toString

  private def formatMillis(millis: Long): String = {
    val secs = millis / 1000
    if (secs < 60) millis / 1000F + " seconds"
    else if (secs < 3600) zeros(secs / 60) + ":" + zeros(secs % 60) + " minutes"
    else zeros(secs / 3600) + ":" + zeros(secs % 3600 / 60) + ":" + zeros(secs % 60) + " hours"
  }

  private def formatRate(bytes: Long, millis: Long): String = {
    if (millis == 0) "? B/s"
    else if (bytes / millis < 1024) (bytes / 1.024F / millis) + " KB/s"
    else (bytes / 1048.576F / millis) + " MB/s"
  }

  private def count(read: Long, close: Boolean = false) {

    if (bytes + read < bytes) throw new IOException("invalid byte count")
    bytes += read

    if (close || bytes >= next) {
      val millis = (System.nanoTime - nanos) / 1000000
      System.err.print(s"[INFO] Download: ${formatBytes(bytes)} of ${formatBytes(length)} in ${formatMillis(millis)} ${formatRate(bytes, millis)}")

      if (close || !pretty) System.err.println()
      else System.err.print("                    \r")

      next = (bytes / step + 1) * step
    }
  }
}