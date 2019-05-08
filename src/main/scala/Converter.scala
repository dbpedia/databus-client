import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.compress.utils.IOUtils

object Converter {

  def decompress(input: File, output: File): Unit = {
    try {
      println(CompressorStreamFactory.detect(new BufferedInputStream(new FileInputStream(input.toJava))))
      val in: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(input.toJava)))
      val out = new FileOutputStream(output.toJava)
      try {
        IOUtils.copy(in, out)
      }
      finally if (in != null) {
        in.close()
      }
    }
  }

  def getCompressionType(file: File): String ={
    CompressorStreamFactory.detect(new BufferedInputStream(new FileInputStream(file.toJava)))
  }

}


/*
  def decompressing(filepath: String): Unit = {

    val fin = Files.newInputStream(Paths.get(filepath))
    val in = new BufferedInputStream(fin)
    val out = Files.newOutputStream(Paths.get("./converted_files/decompressed"))
    val bzIn = new BZip2CompressorInputStream(in)
    val buffer = new Array[Byte](2048)
    var n = 0
    while ( {
      -1 != (n = bzIn.read(buffer))
    }) out.write(buffer, 0, n)
    out.close()
    bzIn.close()
  }

  @throws[FileNotFoundException]
  @throws[CompressorException]
  def getBufferedReaderForCompressedFile(fileIn: String): BufferedReader = {

    val fin = new FileInputStream(fileIn)
    val bis = new BufferedInputStream(fin)
    val input = new CompressorStreamFactory().createCompressorInputStream(bis)
    val br2 = new BufferedReader(new InputStreamReader(input))

    return br2
  }





    CODE VON JOHANNES
    var gzippedOut: CompressorOutputStream = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream)
    var input: ArchiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(originalInput);
    var input: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(originalInput);

    File targetDir = ...
    try (ArchiveInputStream i = ... create the stream for your format, use buffering...) {
      ArchiveEntry entry = null;
      while ((entry = i.getNextEntry()) != null) {
        if (!i.canReadEntryData(entry)) {
          // log something?
          continue;
        }
        String name = fileName(targetDir, entry);
        File f = new File(name);
        if (entry.isDirectory()) {
          if (!f.isDirectory() && !f.mkdirs()) {
            throw new IOException("failed to create directory " + f);
          }
        } else {
          File parent = f.getParentFile();
          if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("failed to create directory " + parent);
          }
          try (OutputStream o = Files.newOutputStream(f.toPath())) {
            IOUtils.copy(i, o);
          }
        }
      }
    }
}
*/
