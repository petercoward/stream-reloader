package peter.coward.stream.reloader.source

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.joda.time.format.DateTimeFormat
import org.scalatest.{FlatSpec, Matchers}
import peter.coward.stream.reloader.utils.{DayGranularity, HourGranularity, MonthGranularity, YearGranularity}

import scala.concurrent.Await
import scala.concurrent.duration._

class DateSourceSpec extends FlatSpec with Matchers {

  implicit val system = ActorSystem("test-system")
  implicit val materializer = ActorMaterializer()

  private val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  "DateSource" should "stream datetimes at daily granularity" in {

    val startDate = formatter.parseDateTime("01/01/2019")
    val endDate = formatter.parseDateTime("05/01/2019")

    val expected = List(
      formatter.parseDateTime("01/01/2019"),
      formatter.parseDateTime("02/01/2019"),
      formatter.parseDateTime("03/01/2019"),
      formatter.parseDateTime("04/01/2019"),
      formatter.parseDateTime("05/01/2019")
    )

    val dateSource = new DateSource(startDate, endDate, DayGranularity).source

    val future = dateSource.runWith(Sink.seq)
    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected
  }

  it should "stream datetimes at monthly granularity" in {
    val startDate = formatter.parseDateTime("01/01/2019")
    val endDate = formatter.parseDateTime("01/05/2019")

    val expected = List(
      formatter.parseDateTime("01/01/2019"),
      formatter.parseDateTime("01/02/2019"),
      formatter.parseDateTime("01/03/2019"),
      formatter.parseDateTime("01/04/2019"),
      formatter.parseDateTime("01/05/2019")
    )

    val dateSource = new DateSource(startDate, endDate, MonthGranularity).source

    val future = dateSource.runWith(Sink.seq)
    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected
  }

  it should "stream datetimes at hourly granularity" in {
    val hourFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH")

    val startDate = hourFormatter.parseDateTime("01/01/2019 00")
    val endDate = hourFormatter.parseDateTime("01/01/2019 10")

    val expected = List(
      hourFormatter.parseDateTime("01/01/2019 00"),
      hourFormatter.parseDateTime("01/01/2019 01"),
      hourFormatter.parseDateTime("01/01/2019 02"),
      hourFormatter.parseDateTime("01/01/2019 03"),
      hourFormatter.parseDateTime("01/01/2019 04"),
      hourFormatter.parseDateTime("01/01/2019 05"),
      hourFormatter.parseDateTime("01/01/2019 06"),
      hourFormatter.parseDateTime("01/01/2019 07"),
      hourFormatter.parseDateTime("01/01/2019 08"),
      hourFormatter.parseDateTime("01/01/2019 09"),
      hourFormatter.parseDateTime("01/01/2019 10")
    )

    val dateSource = new DateSource(startDate, endDate, HourGranularity).source

    val future = dateSource.runWith(Sink.seq)
    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected
  }

  it should "stream datetimes at yearly granularity" in {
    val startDate = formatter.parseDateTime("01/01/2016")
    val endDate = formatter.parseDateTime("01/01/2019")

    val expected = List(
      formatter.parseDateTime("01/01/2016"),
      formatter.parseDateTime("01/01/2017"),
      formatter.parseDateTime("01/01/2018"),
      formatter.parseDateTime("01/01/2019")
    )

    val dateSource = new DateSource(startDate, endDate, YearGranularity).source

    val future = dateSource.runWith(Sink.seq)
    val result = Await.result(future, 3.seconds)

    result should contain theSameElementsAs expected
  }

}
