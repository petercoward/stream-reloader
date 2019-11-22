package peter.coward.stream.reloader.flow

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.findify.s3mock.S3Mock
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import peter.coward.stream.reloader.messages.Event
import peter.coward.stream.reloader.utils.{Granularity, S3Utils}

import scala.concurrent.Await
import scala.concurrent.duration._

class EventLoaderSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val system = ActorSystem("test-system")
  implicit val materializer = ActorMaterializer()

  val mockS3Api = S3Mock(port = 8001)

  val s3Endpoint = new EndpointConfiguration("http://localhost:8001", "eu-west-1")

  val mockBucket = "test-bucket"

  val client = AmazonS3ClientBuilder
    .standard
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(s3Endpoint)
    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
    .build

  val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  val dayGranularity = Granularity.getByName("days")

  override def beforeAll(): Unit = {
    mockS3Api.start
    client.createBucket(mockBucket)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    mockS3Api.stop
    super.afterAll()
  }

  def mockGetEvents(path: String) =
    List("fake data 1", "fake data 2")

  "EventLoader" should "take in a datetime and output an Event" in {

    val eventTypes = Map("eventA" -> List("1.0.0"))
    val prefix = "some/prefix"

    val expected = List(
      Event("eventA", "1.0.0", "fake data 1"),
      Event("eventA", "1.0.0", "fake data 2")
    )

    val eventLoader = new EventLoader(prefix, eventTypes, dayGranularity.datePath(false, _), mockGetEvents).flow

    val dateTime = List(formatter.parseDateTime("01/01/2019"))

    val future = Source(dateTime)
      .via(eventLoader)
      .runWith(Sink.seq)

    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected
  }

  it should "take in a sequence of datetimes and output Events" in {
    val eventTypes = Map("eventA" -> List("1.0.0", "1.1.0"), "eventB" -> List("2.0.0"))

    val expected = List(
      Event("eventA", "1.0.0", "fake data 1"),
      Event("eventA", "1.0.0", "fake data 2"),
      Event("eventA", "1.0.0", "fake data 1"),
      Event("eventA", "1.0.0", "fake data 2"),
      Event("eventA", "1.1.0", "fake data 1"),
      Event("eventA", "1.1.0", "fake data 2"),
      Event("eventA", "1.1.0", "fake data 1"),
      Event("eventA", "1.1.0", "fake data 2"),
      Event("eventB", "2.0.0", "fake data 1"),
      Event("eventB", "2.0.0", "fake data 2"),
      Event("eventB", "2.0.0", "fake data 1"),
      Event("eventB", "2.0.0", "fake data 2")
    )

    val prefix = "some/prefix2"

    val dateTime = List(
      formatter.parseDateTime("01/01/2019"),
      formatter.parseDateTime("02/01/2019")
    )

    val eventLoader = new EventLoader(prefix, eventTypes, dayGranularity.datePath(true, _), mockGetEvents).flow

    val future = Source(dateTime)
      .via(eventLoader)
      .runWith(Sink.seq)

    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected
  }

  it should "take in a datetime and load an event from S3" in {
    val eventTypes = Map("eventA" -> List("1.0.0"))

    val prefix = "some/prefix"

    val data = "this is my fake data"

    val dateTime = List(formatter.parseDateTime("01/01/2019"))

    eventTypes foreach {
      case (event, versions) =>
        versions foreach { version =>
          dateTime.foreach { dt =>
            val year = DateTimeFormat.forPattern("yyyy").print(dt)
            val month = DateTimeFormat.forPattern("MM").print(dt)
            val day = DateTimeFormat.forPattern("dd").print(dt)

            val key = s"$prefix/$event/$version/year=$year/month=$month/day=$day/file.txt"

            println(key)
            client.putObject(mockBucket, key, data)
          }
        }
    }

    val expected = List(
      Event("eventA", "1.0.0", "this is my fake data")
    )

    val s3 = new S3Utils(client, mockBucket)

    val eventLoader = new EventLoader(prefix, eventTypes, dayGranularity.datePath(true, _), s3.getFilesInPath).flow

    val future = Source(dateTime)
      .via(eventLoader)
      .runWith(Sink.seq)

    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected

  }

  it should "take in a sequence of datetimes and load events from S3" in {

  }

}
