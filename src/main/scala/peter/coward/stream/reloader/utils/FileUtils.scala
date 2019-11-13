package peter.coward.stream.reloader.utils

trait FileUtils {
  protected def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B = {
    try f(param) finally param.close
  }

  def getGZipFile(path: String): List[String]

  def getGZipFilesInPath(path: String): List[String]

  def getFile(path: String): List[String]

  def getFilesInPath(path: String): List[String]

  protected def filesInPath(path: String, fileLoader: String => List[String]): List[String]
}
