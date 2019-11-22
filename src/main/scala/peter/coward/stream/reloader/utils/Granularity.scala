package peter.coward.stream.reloader.utils

import org.joda.time._
import org.joda.time.base.BaseSingleFieldPeriod
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import peter.coward.stream.reloader.utils.Granularity._

sealed trait Granularity[A <: BaseSingleFieldPeriod] {
  def name: String
  def between: (ReadableInstant, ReadableInstant) => A
  protected def pattern: DateTimeFormatter

  /**
    * From a datetime, generate a folder path. Will vary based upon what granularity is used.
    * For example, the [[MonthGranularity]] format pattern is yyyy/MM, so given a datetime of
    * 2019/01/01 00:00:00 this function will return either:
    *   "2019/01"
    * or (if partitionNames is true):
    *   "year=2019/month=01"
    * @param partitionNames flag to indicate whether partition names should be part of the returned path
    * @param dt the datetime to generate a path from
    * @return a string folder path
    */
  def datePath(partitionNames: Boolean, dt: DateTime): String = {
    val datetime = pattern.print(dt)

    if (partitionNames) {
      val timeUnits = datetime.split("/").toList
      timeUnits
        .zip(granularities.take(timeUnits.length))
        .map {
          case (timeUnit, granularity) => s"$granularity=$timeUnit"
        }.mkString("/")
    } else {
      datetime
    }
  }
}

case object YearGranularity extends Granularity[Years] {
  def name = "years"
  override def between: (ReadableInstant, ReadableInstant) => Years = Years.yearsBetween
  override protected def pattern = DateTimeFormat.forPattern("yyyy")
}

case object MonthGranularity extends Granularity[Months] {
  def name = "months"
  override def between: (ReadableInstant, ReadableInstant) => Months = Months.monthsBetween
  override protected def pattern = DateTimeFormat.forPattern("yyyy/MM")
}

case object DayGranularity extends Granularity[Days] {
  def name = "days"
  override def between: (ReadableInstant, ReadableInstant) => Days = Days.daysBetween
  override protected def pattern = DateTimeFormat.forPattern("yyyy/MM/dd")
}

case object HourGranularity extends Granularity[Hours] {
  def name = "hours"
  override def between: (ReadableInstant, ReadableInstant) => Hours = Hours.hoursBetween
  override protected def pattern = DateTimeFormat.forPattern("yyyy/MM/dd/HH")
}

object Granularity {
  def getByName(name: String): Granularity[T] forSome { type T <: BaseSingleFieldPeriod } = {
    name match {
      case "years" => YearGranularity
      case "months" => MonthGranularity
      case "days" => DayGranularity
      case "hours" => HourGranularity
    }
  }

  val granularities = List("year", "month", "day", "hour")
}