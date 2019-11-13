package peter.coward.stream.reloader.utils

import java.io.ByteArrayInputStream

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import io.findify.s3mock.S3Mock
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import peter.coward.stream.reloader.TestUtils._

class S3UtilsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val mockS3Api = S3Mock(port = 8002)

  val s3Endpoint = new EndpointConfiguration("http://localhost:8002", "eu-west-1")

  val mockBucket = "test-bucket"

  val client = AmazonS3ClientBuilder
    .standard
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(s3Endpoint)
    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
    .build

  override def beforeAll(): Unit = {
    mockS3Api.start
    client.createBucket(mockBucket)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    mockS3Api.stop
    super.afterAll()
  }

  "S3Utils" should "download and decompress a gzipped file from S3" in {
    val expectedResult = "Hello world, this string will be gzipped"

    val gzippedExpected = stringToGzip(expectedResult)

    val key = "test.gz"

    client.putObject(mockBucket, key, new ByteArrayInputStream(gzippedExpected), new ObjectMetadata())

    val s3 = new S3Utils(client, mockBucket)

    val result = s3.getGZipFile(key)

    result should contain theSameElementsAs List(expectedResult)
  }

  it should "download and decompress all gzipped files in a path in S3" in {
    val file1 = "hello this is file one and it has one line"
    val file2 = "ok this is file two\nand it has two lines"

    val files = Seq((file1, "file1.gz"), (file2, "file2.gz"))

    val path = "path/to/gzip/folder"

    files.foreach {
      case (file, name) =>
        val gz = stringToGzip(file)
        val bis = new ByteArrayInputStream(gz)
        client.putObject(mockBucket, s"$path/$name", bis, new ObjectMetadata())
    }

    val expected = files.flatMap {
      case (f, _) => f.split("\n")
    }.toList

    val s3 = new S3Utils(client, mockBucket)

    val result = s3.getGZipFilesInPath(path)

    result should contain theSameElementsAs expected
  }

  it should "download a file from S3" in {
    val file = "{\"field1\": \"value1\", \"field2\": \"value2\"}"
    val key = "testFile.json"
    val path = "path/to/folder"

    val expected = Seq(file)

    client.putObject(mockBucket, s"$path/$key", file)

    val s3 = new S3Utils(client, mockBucket)

    val result = s3.getFile(s"$path/$key")

    result should contain theSameElementsAs expected
  }

  it should "download all files in a path in S3" in {
    val file1 = "{\"field1\": \"value1\", \"field2\": \"value2\"}"
    val file2 = "{\"field1\": \"value3\", \"field2\": \"value4\"}\n{\"field1\": \"value5\", \"field2\": \"value6\"}"


    val path = "path/to/standard/folder"

    val files = Seq((file1, "file1.json"), (file2, "file2.json"))

    files.foreach{
      case (file, name) =>
        client.putObject(mockBucket, s"$path/$name", file)
    }

    val expected = files.flatMap {
      case (f, _) => f.split("\n")
    }.toList

    val s3 = new S3Utils(client, mockBucket)

    val result = s3.getFilesInPath(path)

    result should contain theSameElementsAs expected
  }
}
