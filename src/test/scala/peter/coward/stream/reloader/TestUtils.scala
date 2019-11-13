package peter.coward.stream.reloader

import java.io._
import java.util.zip.GZIPOutputStream

object TestUtils {

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new IOException(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  def stringToGzip(in: String): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(bos)
    val inBytes = in.getBytes("UTF-8")
    gzip.write(inBytes, 0, inBytes.length)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)

    if (!file.exists) file.createNewFile()

    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }

  def writeFile(filename: String, bytes: Array[Byte]): Unit = {
    val file = new File(filename)
    if (!file.exists) file.createNewFile()

    val bos = new BufferedOutputStream(new FileOutputStream(filename))
    bos.write(bytes)
    bos.close()
  }
}
