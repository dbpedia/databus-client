import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream}

import better.files.File
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorOutputStream, CompressorStreamFactory}
import org.apache.commons.compress.utils.IOUtils

object Converter {

  def getCompressionType(file: File): String ={
    CompressorStreamFactory.detect(new BufferedInputStream(new FileInputStream(file.toJava)))
  }

  def decompress(file: File): CompressorInputStream = {
    try {
      println(CompressorStreamFactory.detect(new BufferedInputStream(new FileInputStream(file.toJava))))
      val in: CompressorInputStream = new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(file.toJava)))
      return in
    }
    // INPUTSTREAM OFFEN LASSEN??
  }

  def convertFormat(input: InputStream, outputFormat:String)={
    val convertedStream = input
  }

  def compress(inputStream: InputStream, outputCompression:String, output:File) = {
    try {
      val myOutputStream = new FileOutputStream(output.toJava)

      val out: CompressorOutputStream = outputCompression match{
        case "bz2" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2, myOutputStream)
        case "gz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, myOutputStream)
        case "br" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BROTLI, myOutputStream)
        case "deflate" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE, myOutputStream)
        case "deflate64" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.DEFLATE64, myOutputStream)
        case "lz4-block" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_BLOCK, myOutputStream)
        case "lz4-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZ4_FRAMED, myOutputStream)
        case "lzma" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.LZMA, myOutputStream)
        case "pack200" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.PACK200, myOutputStream)
        case "snappy-framed" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, myOutputStream)
        case "snappy-raw" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_RAW, myOutputStream)
        case "xz" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.XZ, myOutputStream)
        case "z" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.Z, myOutputStream)
        case "zstd" => new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, myOutputStream)
      }

      try {
        IOUtils.copy(inputStream, out)
      }
      finally if (out != null) {
        out.close()
      }
    }
  }

}

/*  def decompress(input: File, output: File): Unit = {
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
  }*/

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
