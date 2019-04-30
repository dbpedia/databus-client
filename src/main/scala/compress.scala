
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException

import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorOutputStream, CompressorStreamFactory}

object compress {
/*
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
