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

  /**
    * A function to get the right file loading function depending on whether the source data is gzipped
    * @param gzipped A boolean stating whether the source data is gzipped
    * @return either [[getFilesInPath]] or [[getGZipFilesInPath]] functions depending on the value of gzipped
    */
  def getFileLoader(gzipped: Boolean): String => List[String] = {
    if(gzipped) getGZipFilesInPath
    else getFilesInPath
  }
}
