import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, ByteArrayOutputStream, File, FileInputStream, FileNotFoundException, FileOutputStream, IOException, InputStream, InputStreamReader}

import org.apache.commons.compress.compressors.{CompressorException, CompressorInputStream, CompressorOutputStream, CompressorStreamFactory}
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.utils.IOUtils

object ConvertCompression{


  def decompress(input:File, output:File): Unit = {
    try {
      val in:BZip2CompressorInputStream = new BZip2CompressorInputStream(new FileInputStream(input))
      val out:FileOutputStream = new FileOutputStream(output)
      try {
        IOUtils.copy(in, out)
      }
      finally if (in != null) {
        in.close()
      }
    }
  }

  def decompressing(filepath:String): Unit ={

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

/*  def decompressingfail(input:String, output:File): Unit ={
    var out:FileOutputStream = new FileOutputStream(output)
    var in:BufferedReader= getBufferedReaderForCompressedFile(input)

    try {
      var line: String = " "
      while((line=in.readLine())!=null){
        println(line)
      }
      //IOUtils.copy(in, out)
    }
    finally if (in != null) {
      in.close()
    }
  }
*/


  /*
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
  */
}
