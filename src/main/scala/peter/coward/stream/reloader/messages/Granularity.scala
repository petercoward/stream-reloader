package peter.coward.stream.reloader.messages

import org.joda.time.{Days, Hours, Months, ReadableInstant, Years}
import org.joda.time.base.BaseSingleFieldPeriod

sealed trait Granularity[A <: BaseSingleFieldPeriod] {
  def name: String
  def between: (ReadableInstant, ReadableInstant) => A
}

case object YearGranularity extends Granularity[Years] {
  def name = "years"

  override def between: (ReadableInstant, ReadableInstant) => Years = Years.yearsBetween
}

case object MonthGranularity extends Granularity[Months] {
  def name = "months"

  override def between: (ReadableInstant, ReadableInstant) => Months = Months.monthsBetween
}

case object DayGranularity extends Granularity[Days] {
  def name = "days"

  override def between: (ReadableInstant, ReadableInstant) => Days = Days.daysBetween
}

case object HourGranularity extends Granularity[Hours] {
  def name = "hours"

  override def between: (ReadableInstant, ReadableInstant) => Hours = Hours.hoursBetween
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
}