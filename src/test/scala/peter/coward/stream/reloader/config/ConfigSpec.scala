package peter.coward.stream.reloader.config

import org.joda.time.format.DateTimeFormat
import org.scalatest.{FlatSpec, Matchers}

class ConfigSpec extends FlatSpec with Matchers {

  "Config" should "parse command line arguments" in {
    //Test that the implicit converters actually work as intended

    val args = Seq("--startDate",
      "20/01/2019",
      "--endDate",
      "01/04/2019",
      "--granularity",
      "days",
      "--partitionNames",
      "true",
      "--events",
      "EventName1=1.0.1|1.0.2,EventName2=2.0.1|2.0.2",
      "--sourceBucket",
      "my-bucket",
      "--sourcePrefix",
      "myPrefix",
      "--destination",
      "dest-topic",
      "--brokerList",
      "0.0.0.0,0.0.0.1,0.0.0.2"
    )

    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")

    val eventsMap = Map(
      "EventName1" -> List("1.0.1", "1.0.2"),
      "EventName2" -> List("2.0.1", "2.0.2")
    )

    val expected = Config(
      startDate = formatter.parseDateTime("20/01/2019"),
      endDate = Some(formatter.parseDateTime("01/04/2019")),
      granularity = "days",
      partitionNames = true,
      events = eventsMap,
      sourceBucket = "my-bucket",
      sourcePrefix = "myPrefix",
      destinationTopic = "dest-topic",
      brokerList = "0.0.0.0,0.0.0.1,0.0.0.2"
    )

    val actual = Config.apply(args.toArray)

    actual shouldEqual expected
  }

}
