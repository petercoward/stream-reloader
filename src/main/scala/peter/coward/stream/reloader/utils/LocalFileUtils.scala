package peter.coward.stream.reloader.utils

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import scala.io.Source

class LocalFileUtils extends FileUtils {

  def getGZipFile(path: String): List[String] = {
    def gzis(p: String) = new GZIPInputStream(new FileInputStream(p))

    using(Source.fromInputStream(gzis(path))) { source =>
      source.getLines.toList
    }
  }

  def getGZipFilesInPath(path: String): List[String] = {
    filesInPath(path, getGZipFile)
  }

  def getFile(path: String): List[String] = {
    using(Source.fromFile(path)) { source =>
      source.getLines.toList
    }
  }

  def getFilesInPath(path: String): List[String] = {
    filesInPath(path, getFile)
  }

  protected def filesInPath(path: String, fileLoader: String => List[String]): List[String] = {
    val dir = new File(path)

    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList flatMap { file =>
        fileLoader(file.getPath)
      }
    } else {
      List[String]()
    }
  }
}
