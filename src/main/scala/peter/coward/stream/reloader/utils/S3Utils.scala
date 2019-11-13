package peter.coward.stream.reloader.utils

import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3

import scala.io.Source
import scala.collection.JavaConverters._

class S3Utils(client: AmazonS3, bucket: String) extends FileUtils {

  def getGZipFile(path: String): List[String] = {
    val gzFile = client.getObject(bucket, path)
    val gzIn = new GZIPInputStream(gzFile.getObjectContent)

    using(Source.fromInputStream(gzIn)) { source =>
      source.getLines.toList

    }
  }

  def getGZipFilesInPath(path: String): List[String] = {
    filesInPath(path, getGZipFile)
  }

  def getFile(path: String): List[String] = {
    val file = client.getObject(bucket, path)

    using(Source.fromInputStream(file.getObjectContent)) { source =>
      source.getLines.toList
    }
  }

  def getFilesInPath(path: String): List[String] = {
    filesInPath(path, getFile)
  }

  protected def filesInPath(path: String, fileLoader: String => List[String]): List[String] = {
    val files = client.listObjectsV2(bucket, path)
    files.getObjectSummaries.asScala.toList flatMap { os =>
      fileLoader(os.getKey)
    }
  }
}
