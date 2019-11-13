package peter.coward.stream.reloader.utils

import java.io.File

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import peter.coward.stream.reloader.TestUtils._

class LocalFileUtilsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val basePath = "/tmp/stream-reloader-test/"
  val basePathFile = new File(basePath)

  override def beforeAll(): Unit = {
    basePathFile.mkdir()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    deleteRecursively(basePathFile)
    super.afterAll()
  }

  "LocalFileUtils" should "load and decompress a gzipped file from the local file system" in {
    val expectedResult = "Hello world, this string was gzipped"

    val gzippedExpected = stringToGzip(expectedResult)

    val filePath = s"${basePath}test.gz"

    writeFile(filePath, gzippedExpected)

    val fileUtils = new LocalFileUtils

    val result = fileUtils.getGZipFile(filePath)

    result should contain theSameElementsAs List(expectedResult)

  }

  it should "load and decompress all gzipped files in a folder on the local file system" in {
    val file1 = "This is my first file with just one line"
    val file2 = "This second file\nhas two lines"

    val subFolderPath = s"${basePath}multi_gzip/"

    val subFolder = new File(subFolderPath)
    subFolder.mkdir()

    val files = Seq(
      (s"${subFolderPath}file1.gz", file1),
      (s"${subFolderPath}file2.gz", file2)
    )

    files.foreach {
      case (fileName, fileContents) => writeFile(fileName, stringToGzip(fileContents))
    }

    val expected = files.flatMap(f => f._2.split("\n")).toList

    val fileUtils = new LocalFileUtils

    val result = fileUtils.getGZipFilesInPath(subFolderPath)

    result should contain theSameElementsAs expected
  }

  it should "load a file from the local file system" in {
    val expectedResult = "Hello world, this string was gzipped"

    val filePath = s"${basePath}test.txt"

    writeFile(filePath, Seq(expectedResult))

    val fileUtils = new LocalFileUtils

    val result = fileUtils.getFile(filePath)

    result should contain theSameElementsAs List(expectedResult)
  }

  it should "load all files in a folder on the local file system" in {
    val file1 = "This is the first file\nand it has two lines"
    val file2 = "This is the\nsecond file\nand it has three lines"

    val subfolderPath = s"${basePath}multi/"

    val subfolder = new File(subfolderPath)
    subfolder.mkdir()

    val files = Seq(
      (s"${subfolderPath}file1.txt", file1),
      (s"${subfolderPath}file2.txt", file2)
    )

    files.foreach {
      case (fileName, fileContents) => writeFile(fileName, Seq(fileContents))
    }

    val expected = files.flatMap(_._2.split("\n").toList)

    val fileUtils = new LocalFileUtils

    val result = fileUtils.getFilesInPath(subfolderPath)

    result should contain theSameElementsAs expected
  }

}
