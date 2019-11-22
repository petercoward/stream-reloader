package peter.coward.stream.reloader.utils

import org.joda.time.format.DateTimeFormat
import org.scalatest.{FlatSpec, Matchers}

class GranularitySpec extends FlatSpec with Matchers {

  "Year Granularity" should "generate a path with partition names" in {
    val datetime = "01/02/2019"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val dt = formatter.parseDateTime(datetime)

    val expected = "year=2019"

    val result = YearGranularity.datePath(true, dt)

    result shouldBe expected
  }

  it should "generate a path" in {
    val datetime = "01/02/2019"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val dt = formatter.parseDateTime(datetime)

    val expected = "2019"

    val result = YearGranularity.datePath(false, dt)

    result shouldBe expected
  }

  "MonthGranularity" should "generate a path with partition names" in {
    val datetime = "01/02/2019"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val dt = formatter.parseDateTime(datetime)

    val expected = "year=2019/month=02"

    val result = MonthGranularity.datePath(true, dt)

    result shouldBe expected
  }

  it should "generate a path" in {
    val datetime = "01/02/2019"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val dt = formatter.parseDateTime(datetime)

    val expected = "2019/02"

    val result = MonthGranularity.datePath(false, dt)

    result shouldBe expected
  }

  "DayGranularity" should "generate a path with partition names" in {
    val datetime = "01/02/2019"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val dt = formatter.parseDateTime(datetime)

    val expected = "year=2019/month=02/day=01"

    val result = DayGranularity.datePath(true, dt)

    result shouldBe expected
  }

  it should "generate a path" in {
    val datetime = "01/02/2019"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
    val dt = formatter.parseDateTime(datetime)

    val expected = "2019/02/01"

    val result = DayGranularity.datePath(false, dt)

    result shouldBe expected
  }

  "HourGranularity" should "generate a path with partition names" in {
    val datetime = "01/02/2019 22"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH")
    val dt = formatter.parseDateTime(datetime)

    val expected = "year=2019/month=02/day=01/hour=22"

    val result = HourGranularity.datePath(true, dt)

    result shouldBe expected
  }

  it should "generate a path" in {
    val datetime = "01/02/2019 22"
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH")
    val dt = formatter.parseDateTime(datetime)

    val expected = "2019/02/01/22"

    val result = HourGranularity.datePath(false, dt)

    result shouldBe expected
  }

}
