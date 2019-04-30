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



}
